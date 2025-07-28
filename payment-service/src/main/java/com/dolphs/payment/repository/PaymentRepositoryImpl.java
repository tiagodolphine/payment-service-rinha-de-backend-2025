package com.dolphs.payment.repository;

import com.dolphs.payment.domain.model.PaymentMessage;
import com.dolphs.payment.domain.model.PaymentTransaction;
import com.dolphs.payment.domain.model.Summary;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.List;
import java.util.function.Function;

@Repository
public class PaymentRepositoryImpl {
    private final ConnectionPool connectionFactory;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Sinks.Many<PaymentMessage> sink;

    public PaymentRepositoryImpl(@Qualifier("connectionFactory") ConnectionPool connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.sink = Sinks.many().unicast().onBackpressureBuffer(new ArrayDeque<>());
    }

    public Mono<PaymentMessage> save(PaymentMessage paymentMessage) {
        sink.tryEmitNext(paymentMessage);
        return Mono.just(paymentMessage);
    }

    public Mono<Void> processChunk(int chunkSize, Function<PaymentMessage, Mono<PaymentTransaction>> processor) {
        int parallelism = chunkSize; // Number of concurrent processes
        return sink.asFlux()
                .flatMap(message ->
                                processor.apply(message)
                                        .flatMap(this::savePaymentTransaction)
                                        .onErrorResume(e -> {
                                            log.error("Error processing message, re-queuing", e);
                                            save(message).subscribe();
                                            return Mono.empty();
                                        }),
                        parallelism // concurrency level
                )
                .onErrorResume(e -> {
                    log.error("Error processing chunk", e);
                    return Mono.empty();
                })
                .then();
    }

    //Payment Sumary

    public Mono<Summary> getPaymentsSummary(OffsetDateTime from, OffsetDateTime to, int processorId) {
        String sql = """
                SELECT
                    COUNT(*) AS total_requests,
                    COALESCE(SUM(amount), 0) AS total_amount
                FROM payment_transaction
                WHERE ($1 IS NULL OR timestamp >= $1)
                  AND ($2 IS NULL OR timestamp <= $2)
                  AND processor_id = $3
                """;
        return Mono.usingWhen(
                connectionFactory.create(),
                conn -> Mono.from(
                                conn.createStatement(sql)
                                        .bind(0, from)
                                        .bind(1, to)
                                        .bind(2, processorId)
                                        .execute()
                        )
                        .flatMap(result -> Mono.from(result.map((row, metadata) ->
                                new Summary(
                                        row.get("total_requests", Integer.class),
                                        row.get("total_amount", Double.class),
                                        processorId
                                )
                        ))),
                Connection::close
        );
    }

    public Mono<PaymentTransaction> savePaymentTransaction(PaymentTransaction paymentTransaction) {
        String sql = "INSERT INTO payment_transaction (amount, processor_id, timestamp) " +
                "VALUES ($1, $2, $3)";
        return Mono.usingWhen(
                connectionFactory.create(),
                conn -> Mono.from(conn.createStatement(sql)
                                .bind(0, paymentTransaction.getAmount())
                                .bind(1, paymentTransaction.getProcessorId())
                                .bind(2, paymentTransaction.getTimestamp())
                                .execute()
                        )
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .map(r -> paymentTransaction),
                Connection::close);
    }
}