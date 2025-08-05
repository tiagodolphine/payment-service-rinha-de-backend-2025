package com.dolphs;

import com.dolphs.model.PaymentMessage;
import com.dolphs.model.PaymentSummary;
import com.dolphs.model.PaymentTransaction;
import com.dolphs.model.Summary;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.search.CreateArgs;
import io.quarkus.redis.datasource.search.FieldType;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Request;
import io.vertx.mutiny.redis.client.Response;
import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class PaymentQueue {

    ReactiveRedisDataSource reactive;
    Logger log = LoggerFactory.getLogger(PaymentQueue.class);

    public PaymentQueue(ReactiveRedisDataSource reactive) {
        this.reactive = reactive;
        reactive.search().ftCreate("idx:transaction", new CreateArgs().onHash().prefixes("transaction:")
                        .indexedField("processorId", FieldType.TEXT)
                        .indexedField("amount", FieldType.NUMERIC)
                        .indexedField("timestamp", FieldType.NUMERIC))
                .subscribe()
                .with(item -> {
                    log.info("Index created successfully");
                }, failure -> {
                    log.error("Error during index creation: " + failure.getMessage());
                });
    }

    public Uni<Long> enqueue(PaymentMessage message) {
        return reactive.list(PaymentMessage.class).lpush("payments", message);
    }

    public Uni<List<PaymentMessage>> dequeue(int items) {
        return reactive.list(PaymentMessage.class).lpop("payments", items);
    }

    public Uni<Void> saveTransaction(PaymentTransaction paymentTransaction) {
        Map map = Map.of(
                "id", paymentTransaction.getId(),
                "processorId", paymentTransaction.getProcessorId(),
                "amount", paymentTransaction.getAmount(),
                "timestamp", convertDate(paymentTransaction.getTimestamp()));

        return reactive.hash(Map.class)
                .hset("transaction:" + paymentTransaction.getId(), map)
                .replaceWithVoid();
    }

    public static long convertDate(OffsetDateTime offsetDateTime) {
        return offsetDateTime.toInstant().toEpochMilli();
    }

    public Uni<Summary> getSummary(String processorId, OffsetDateTime from, OffsetDateTime to) {
        String query = String.format("@processorId:%s @timestamp:[%d %d]", processorId,
                convertDate(from),
                convertDate(to));

        Request req = Request.cmd(Command.FT_AGGREGATE)
                .arg("idx:transaction")
                .arg(query)
                .arg("GROUPBY").arg("1").arg("@processorId")
                .arg("REDUCE").arg("SUM").arg("1").arg("@amount").arg("AS").arg("total")
                .arg("REDUCE").arg("COUNT").arg("0").arg("AS").arg("count");

        return reactive.getRedis().send(req)
                .map(resp -> {
                    // Parse resp to extract total and count
                    // (parsing logic depends on the response structure)
                    Response results = resp.get("results");
                    if (results.size() == 0) {
                        return new Summary(0, 0, processorId);
                    }
                    Response response = results.get(0).get("extra_attributes");
                    double total = response.get("total").toDouble();
                    int count = response.get("count").toInteger();
                    return new Summary(count, total, processorId);
                });
    }

    public Uni<PaymentSummary> getSummary(OffsetDateTime from, OffsetDateTime to) {
        return Uni.combine().all().unis(
                        getSummary("default", from, to),
                        getSummary("fallback", from, to))
                .with(PaymentSummary::new);
    }
}