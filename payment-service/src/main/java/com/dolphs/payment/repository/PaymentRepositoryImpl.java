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

import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;

@Repository
public class PaymentRepositoryImpl {
    private final ConnectionPool connectionFactory;
    private final ConnectionPool connectionFactory2;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public PaymentRepositoryImpl(@Qualifier("connectionFactory") ConnectionPool connectionFactory,
                                 @Qualifier("connectionFactory2") ConnectionPool connectionFactory2) {
        this.connectionFactory = connectionFactory;
        this.connectionFactory2 = connectionFactory2;
    }

    public Mono<Void> saveBatch(List<PaymentMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return Mono.empty();
        }
        StringBuilder sql = new StringBuilder("INSERT INTO payment_message (amount, correlation_id) VALUES ");
        int paramIndex = 1;
        for (int i = 0; i < messages.size(); i++) {
            sql.append("($").append(paramIndex++).append(", $").append(paramIndex++).append(")");
            if (i < messages.size() - 1) {
                sql.append(", ");
            }
        }

        return Mono.usingWhen(
                connectionFactory2.create(),
                conn -> {
                    var stmt = conn.createStatement(sql.toString());
                    int bindIndex = 0;
                    for (PaymentMessage msg : messages) {
                        stmt.bind(bindIndex++, msg.getAmount());
                        stmt.bind(bindIndex++, msg.getCorrelationId());
                    }
                    return Mono.from(stmt.execute())
                            .flatMap(result -> Mono.from(result.getRowsUpdated())).then();
                },
                Connection::close
        );
    }

    public Mono<PaymentMessage> save(PaymentMessage paymentMessage) {
        return Mono.usingWhen(
                connectionFactory2.create(),
                conn -> Mono.from(conn.createStatement(
                                        "INSERT INTO payment_message (amount, correlation_id) VALUES ($1, $2)")
                                .bind(0, paymentMessage.getAmount())
                                .bind(1, paymentMessage.getCorrelationId())
                                .execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()).map(v -> paymentMessage)),
                Connection::close
        );
    }

    public Flux<PaymentMessage> findNextChunkToProcess(int chunkSize, Connection conn) {
        String query = "SELECT id, amount, correlation_id FROM payment_message ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED";
        return Flux.from(conn.createStatement(query)
                        .bind(0, chunkSize)
                        .execute())
                .flatMap(result -> Flux.from(result.map((row, metadata) -> mapRow(row))));
    }

    public Mono<Void> delete(Long id, Connection conn) {
        return Mono.from(conn.createStatement("DELETE FROM payment_message WHERE id = $1")
                        .bind(0, id)
                        .execute())
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .then();
    }

    private PaymentMessage mapRow(Row row) {
        PaymentMessage msg = new PaymentMessage(
                row.get("amount", Double.class),
                row.get("correlation_id", String.class),
                row.get("id", Long.class)
        );
        return msg;
    }

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

    public Mono<PaymentTransaction> savePaymentTransaction(PaymentTransaction paymentTransaction, Connection conn) {
        String sql = "INSERT INTO payment_transaction (amount, processor_id, timestamp) " +
                "VALUES ($1, $2, $3)";
        return Mono.from(
                        conn.createStatement(sql)
                                .bind(0, paymentTransaction.getAmount())
                                .bind(1, paymentTransaction.getProcessorId())
                                .bind(2, paymentTransaction.getTimestamp())
                                .execute()
                )
                .flatMap(r -> Mono.from(r.getRowsUpdated()))
                .map(r -> paymentTransaction);
    }

    public Mono<Void> processChunk(int chunkSize, Function<PaymentMessage, Mono<PaymentTransaction>> processor) {
        return Mono.usingWhen(connectionFactory.create(),
                conn -> Mono.from(conn.beginTransaction())
                        .thenMany(findNextChunkToProcess(chunkSize, conn))
                        .flatMap(processor)
                        .flatMap(paymentMessage -> savePaymentTransaction(paymentMessage, conn))
                        .flatMap(paymentMessage -> delete(paymentMessage.getMessageId(), conn))
                        .then(Mono.from(conn.commitTransaction()))
                        .onErrorResume(c-> Mono.from(conn.rollbackTransaction()).then(Mono.empty()))
                        .onErrorContinue((e, o) -> {
                            log.error("Error processing chunk", e);
                        }), Connection::close);
    }
}