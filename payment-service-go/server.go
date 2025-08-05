package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/go-redis/redis/v8"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/joho/godotenv"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type PaymentMessage struct {
	Amount        float64 `json:"amount"`
	CorrelationId string  `json:"correlationId"`
}

type PaymentTransaction struct {
	Amount        float64 `json:"amount"`
	CorrelationId string  `json:"correlationId"`
	ProcessorId   string  `json:"processorId"`
	ProcessedAt   int64   `json:"processedAt"`
}

type TransactionKey struct {
	ProcessedAt   time.Time
	CorrelationId string
}

type Summary struct {
	Count       int     `json:"totalRequests"`
	Total       float64 `json:"totalAmount"`
	ProcessorId string  `json:"-"`
}

type PaymentSummary struct {
	DefaultSummary Summary `json:"default"`
	Fallback       Summary `json:"fallback"`
}

type Health struct {
	Failing         bool  `json:"failing"`
	MinResponseTime int64 `json:"minResponseTime"`
}

var (
	env         = godotenv.Load(".env")
	ctx         = context.Background()
	isoFormat   = "2006-01-02T15:04:05.000Z"
	poolSize, _ = strconv.Atoi(os.Getenv("REDIS_MAX_POOL_SIZE"))

	redisHost = os.Getenv("REDIS_HOST")
	rdb       = redis.NewClient(&redis.Options{
		Addr:               redisHost + ":6379",
		DB:                 0,
		PoolSize:           poolSize, // Increase pool size
		MinIdleConns:       50,       // Maintain idle connections
		DialTimeout:        2 * time.Second,
		ReadTimeout:        2 * time.Second,
		WriteTimeout:       2 * time.Second,
		IdleCheckFrequency: -1,
	})
	queueName       = "payments"
	defaultURL      = os.Getenv("PAYMENT_PROCESSOR_DEFAULT")
	fallbackURL     = os.Getenv("PAYMENT_PROCESSOR_FALLBACK")
	useDefault      atomic.Bool
	workers, _      = strconv.Atoi(os.Getenv("WORKERS"))
	pollInterval, _ = strconv.Atoi(os.Getenv("POLL_INTERVAL"))
	paymentQueue    chan PaymentMessage
	customTransport = &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		MaxConnsPerHost:     100,
	}
	client = &http.Client{
		Transport: customTransport,
		Timeout:   1000 * time.Millisecond,
	}
)

func main() {
	paymentQueue = make(chan PaymentMessage, 20000)
	useDefault.Store(true)
	// go healthCheckLoop()

	for i := 1; i <= workers; i++ {
		//transactionMap[i] = redblacktree.NewWith(transactionKeyComparator)
		go paymentWorker(i)
	}

	err := createIndex(rdb, ctx)
	if err != nil {
		if err.Error() == "Index already exists" {
			print("index already exists, skipping creation")
		} else {
			print(err.Error())
		}
	} else {
		print("create index success")
	}

	app := fiber.New(fiber.Config{
		JSONEncoder: json.Marshal,
		JSONDecoder: json.Unmarshal,
	})

	app.Post("/payments", func(c *fiber.Ctx) error {
		var payment PaymentMessage
		if err := c.BodyParser(&payment); err != nil {
			return c.Status(400).SendString("Invalid payload")
		}
		if c.Query("generate") == "true" {
			payment.CorrelationId = utils.UUID()
		}

		go createPayment(payment)

		return c.SendStatus(fiber.StatusOK)
	})

	app.Get("/payments-summary", func(c *fiber.Ctx) error {
		fromStr := c.Query("from", time.Now().Add(-1*time.Hour).Format(isoFormat))
		toStr := c.Query("to", time.Now().Format(isoFormat))
		from, err := time.Parse(isoFormat, fromStr)
		if err != nil {
			return c.Status(400).SendString("Invalid 'from' time")
		}
		to, err := time.Parse(isoFormat, toStr)
		if err != nil {
			return c.Status(400).SendString("Invalid 'to' time")
		}
		var summary = getPaymentSummary(from, to)
		var body []byte
		body, err = json.Marshal(summary)
		if err != nil {
			return err
		}
		return c.Status(fiber.StatusOK).Send(body)
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	app.Listen(":8080")
}

// server.go

func getPaymentSummary(from, to time.Time) PaymentSummary {
	type result struct {
		summary *Summary
		name    string
	}
	ch := make(chan result, 2)

	go func() {
		s, _ := getSummaryRedis("default", from, to)
		ch <- result{summary: s, name: "default"}
	}()
	go func() {
		s, _ := getSummaryRedis("fallback", from, to)
		ch <- result{summary: s, name: "fallback"}
	}()

	var defaultSummary, fallbackSummary *Summary
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.name == "default" {
			defaultSummary = r.summary
		} else {
			fallbackSummary = r.summary
		}
	}

	if defaultSummary == nil {
		defaultSummary = &Summary{ProcessorId: "default"}
	}
	if fallbackSummary == nil {
		fallbackSummary = &Summary{ProcessorId: "fallback"}
	}
	return PaymentSummary{
		DefaultSummary: *defaultSummary,
		Fallback:       *fallbackSummary,
	}
}

func createPayment(payment PaymentMessage) {
	paymentQueue <- payment
}

func healthCheckLoop() {
	for {
		healthy := checkHealth(defaultURL)
		useDefault.Store(healthy)
		time.Sleep(3 * time.Second)
	}
}

func checkHealth(url string) bool {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/payments/service-health")
	if err != nil || resp.StatusCode != 200 {
		return false
	}
	defer resp.Body.Close()
	var result Health
	json.NewDecoder(resp.Body).Decode(result)
	return !result.Failing
}

func paymentWorker(id int) {
	for {
		payment, ok := <-paymentQueue
		if !ok {
			fmt.Printf("Worker %d: Payment queue closed, \n", id)
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			continue
		}
		processPayment(payment, id)
		minInterval := pollInterval / 2
		time.Sleep(time.Duration(rand.Intn(pollInterval-minInterval)+minInterval) * time.Millisecond)
	}
}

func processPayment(payment PaymentMessage, workerId int) {
	url := defaultURL
	ProcessorId := "default"
	if !useDefault.Load() {
		url = fallbackURL
		ProcessorId = "fallback"
	}
	RequestedAt := time.Now().In(time.UTC)
	payload, _ := json.Marshal(map[string]interface{}{
		"amount":        payment.Amount,
		"correlationId": payment.CorrelationId,
		"requestedAt":   RequestedAt.Format(isoFormat),
	})
	req, err := http.NewRequest("POST", url+"/payments", bytes.NewReader(payload))
	if err != nil {
		fmt.Printf("Error creating request for payment %s: %v\n", payment.CorrelationId, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	pipe := beginRedisTransaction()
	queueSaveTransaction(pipe, PaymentTransaction{
		Amount:        payment.Amount,
		CorrelationId: payment.CorrelationId,
		ProcessorId:   ProcessorId,
		ProcessedAt:   RequestedAt.UnixMilli(),
	})

	resp, err := client.Do(req)

	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		abortRedisTransaction(pipe)
		time.Sleep(time.Duration(2000) * time.Millisecond)
		createPayment(payment)
		return
	}
	if resp.StatusCode >= 500 {
		abortRedisTransaction(pipe)
		time.Sleep(time.Duration(2000) * time.Millisecond)
		createPayment(payment)
		return
	}
	err = commitRedisTransaction(pipe)
	if err != nil {
		rollbackRedisTransaction(payment.CorrelationId)
	}
}

func transactionKeyComparator(a, b interface{}) int {
	ak := a.(TransactionKey)
	bk := b.(TransactionKey)
	if ak.ProcessedAt.Before(bk.ProcessedAt) {
		return -1
	}
	if ak.ProcessedAt.After(bk.ProcessedAt) {
		return 1
	}
	// If times are equal, compare CorrelationId
	return bytes.Compare([]byte(ak.CorrelationId), []byte(bk.CorrelationId))
}

func getTransactionsInRangeM(from, to time.Time, processorId string, transactionTree *redblacktree.Tree) (*Summary, error) {
	it := transactionTree.Iterator()
	var count int
	var total float64
	for it.Next() {
		key := it.Key().(TransactionKey)
		transaction := it.Value().(PaymentTransaction)
		if processorId != transaction.ProcessorId || key.ProcessedAt.Before(from) {
			continue
		}
		if key.ProcessedAt.After(to) {
			break
		}
		total += transaction.Amount
		count += 1
	}
	return &Summary{Count: count, Total: total, ProcessorId: processorId}, nil
}

// Redis
func beginRedisTransaction() redis.Pipeliner {
	return rdb.TxPipeline()
}

// Queue saving a PaymentTransaction in the pipeline
func queueSaveTransaction(pipe redis.Pipeliner, tx PaymentTransaction) {
	key := "transaction:" + tx.CorrelationId
	fields := map[string]interface{}{
		"id":          tx.CorrelationId,
		"processorId": tx.ProcessorId,
		"amount":      tx.Amount,
		"timestamp":   tx.ProcessedAt,
	}
	pipe.HSet(ctx, key, fields)
}

// Commit the transaction
func commitRedisTransaction(pipe redis.Pipeliner) error {
	_, err := pipe.Exec(ctx)
	return err
}

// Rollback: delete the transaction if needed
func rollbackRedisTransaction(id string) error {
	key := "transaction:" + id
	return rdb.Del(ctx, key).Err()
}

func abortRedisTransaction(pipe redis.Pipeliner) error {
	return pipe.Discard()
}

// Enqueue a payment message
func EnqueuePayment(msg PaymentMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return rdb.LPush(ctx, queueName, data).Err()
}

func createIndex(rdb *redis.Client, ctx context.Context) error {
	cmd := []interface{}{
		"idx:transaction",
		"ON", "HASH",
		"PREFIX", "1", "transaction:",
		"SCHEMA",
		"processorId", "TEXT",
		"amount", "NUMERIC",
		"timestamp", "NUMERIC",
	}
	res := rdb.Do(ctx, append([]interface{}{"FT.CREATE"}, cmd...)...)
	return res.Err()
}

// Save transaction as hash and add to sorted set for time range queries
func saveTransactionRedis(tx PaymentTransaction) error {
	key := "transaction:" + tx.CorrelationId
	fields := map[string]interface{}{
		"id":          tx.CorrelationId,
		"processorId": tx.ProcessorId,
		"amount":      tx.Amount,
		"timestamp":   tx.ProcessedAt,
	}
	return rdb.HSet(ctx, key, fields).Err()
}

func deleteTransactionRedis(correlationId string) error {
	key := "transaction:" + correlationId
	return rdb.Del(ctx, key).Err()
}

// Generate summary for processorId in time range
func getSummaryRedis(processorID string, from, to time.Time) (*Summary, error) {
	query := fmt.Sprintf("@processorId:%s @timestamp:[%d %d]", processorID, from.UTC().UnixMilli(), to.UTC().UnixMilli())
	args := []interface{}{
		"idx:transaction",
		query,
		"GROUPBY", "1", "@processorId",
		"REDUCE", "SUM", "1", "@amount", "AS", "total",
		"REDUCE", "COUNT", "0", "AS", "count",
	}
	res, err := rdb.Do(ctx, append([]interface{}{"FT.AGGREGATE"}, args...)...).Result()
	if err != nil {
		return nil, err
	}
	results, ok := res.([]interface{})
	if !ok || len(results) < 2 {
		return &Summary{Count: 0, Total: 0, ProcessorId: processorID}, nil
	}
	row, ok := results[1].([]interface{})
	if !ok {
		return &Summary{Count: 0, Total: 0, ProcessorId: processorID}, nil
	}
	var total float64
	var count int
	for i := 0; i < len(row)-1; i += 2 {
		key, _ := row[i].(string)
		switch key {
		case "total":
			total, _ = strconv.ParseFloat(row[i+1].(string), 64)
		case "count":
			count, _ = strconv.Atoi(row[i+1].(string))
		}
	}
	return &Summary{Count: count, Total: total, ProcessorId: processorID}, nil
}
