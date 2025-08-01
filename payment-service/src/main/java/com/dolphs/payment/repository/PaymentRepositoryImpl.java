package com.dolphs.payment.repository;

import com.dolphs.payment.domain.model.PaymentMessage;
import com.dolphs.payment.domain.model.PaymentTransaction;
import com.dolphs.payment.domain.model.Summary;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.Serializable;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

@Repository
public class PaymentRepositoryImpl implements Serializable {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final BlockingQueue<PaymentMessage> queue;
    private final IMap<String, PaymentTransaction> transactions;
    private final IMap<String, PaymentTransaction> fallbackTransactions;
    private final HazelcastInstance hz;
    private static final long serialVersionUID = 1L;

    public PaymentRepositoryImpl(HazelcastInstance client) {
        this.hz = client;
        // Get a Blocking Queue called "my-distributed-queue"
        queue = hz.getQueue("payments-queue");
        transactions = hz.getMap("payments-transactions");
        fallbackTransactions = hz.getMap("payments-transactions-fallback");
    }

    public Mono<PaymentMessage> save(PaymentMessage paymentMessage) {
        return Mono.just(paymentMessage).doOnNext(p->
                Mono.just(queue.offer(paymentMessage)).subscribe());
    }

    public Mono<Void> processChunk(int chunkSize, Function<PaymentMessage, Mono<PaymentTransaction>> processor) {
        int parallelism = chunkSize;
        return Flux.defer(() -> Flux.fromIterable(pollMessages(chunkSize)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(message ->
                                processor.apply(message)
                                        .doOnNext(t -> savePaymentTransaction(t).subscribeOn(Schedulers.boundedElastic()).subscribe())
                                        .onErrorResume(e -> {
                                            log.error("Error processing message, re-queuing", e);
                                            save(message).delayElement(Duration.ofSeconds(2)).subscribe();
                                            return Mono.empty();
                                        }),
                        parallelism, parallelism
                )
                .then();
    }

    private List<PaymentMessage> pollMessages(int chunkSize) {
        List<PaymentMessage> batch = new ArrayList<>(chunkSize);
        for (int i = 0; i < chunkSize; i++) {
            PaymentMessage msg = queue.poll();
            if (msg == null) break;
            batch.add(msg);
        }
        return batch;
    }

    //Payment Sumary

    @RegisterReflectionForBinding({Summary.class, PaymentTransaction.class, TransactionAggregator.class, Aggregator.class})
    public Mono<Summary> getPaymentsSummary(OffsetDateTime from, OffsetDateTime to, int processorId) {
        Predicate<String, PaymentTransaction> criteriaQuery = Predicates.and(
                Predicates.between("timestamp", from, to)
        );
        IMap<String, PaymentTransaction> targetMap = processorId == 1 ? transactions : fallbackTransactions;
        Aggregator<Map.Entry<String, PaymentTransaction>, Double> aggregator = Aggregators.doubleSum("amount");
        Aggregator<Map.Entry<String, PaymentTransaction>, Long> countAggregator = Aggregators.count();
        var sum = targetMap.aggregate(aggregator, criteriaQuery);
        var count = targetMap.aggregate(countAggregator, criteriaQuery);
        return Mono.just(new Summary(count.intValue(), sum, processorId));
    }

    public Mono<PaymentTransaction> savePaymentTransaction(PaymentTransaction paymentTransaction) {
        if (paymentTransaction.getProcessorId() < 0) {
            return Mono.empty();
        }
        IMap<String, PaymentTransaction> targetMap = paymentTransaction.getProcessorId() == 1 ? transactions : fallbackTransactions;
        return Mono.just(paymentTransaction).doOnNext(p ->
                Mono.fromCallable(()-> targetMap.put(paymentTransaction.getId(), paymentTransaction)).subscribe());
    }

    public Mono<PaymentTransaction> deletePaymentTransaction(PaymentTransaction paymentTransaction) {
        if (paymentTransaction.getProcessorId() < 0) {
            return Mono.empty();
        }
        IMap<String, PaymentTransaction> targetMap = paymentTransaction.getProcessorId() == 1 ? transactions : fallbackTransactions;
        return Mono.just(paymentTransaction).doOnNext(p ->
                Mono.fromCallable(()-> targetMap.remove(paymentTransaction.getId())).subscribe());
    }
}