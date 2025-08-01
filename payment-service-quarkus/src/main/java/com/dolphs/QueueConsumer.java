package com.dolphs;

import com.dolphs.model.PaymentMessage;
import com.dolphs.model.PaymentTransaction;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.virtual.threads.VirtualThreads;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.ExecutorService;

@Singleton
@Startup
public class QueueConsumer {

    @Inject
    PaymentQueue queue;
    @ConfigProperty(name = "chunkSize")
    int chunkSize;
    @ConfigProperty(name = "pollInterval")
    long pollInterval;
    @Inject
    PaymentProcessor processor;
    @Inject
    Vertx vertx;
    @Inject
    @VirtualThreads
    ExecutorService vThreads;

    Logger log = LoggerFactory.getLogger(QueueConsumer.class);

    public void processMessages(@Observes StartupEvent ev) {
        Multi.createFrom().ticks().every(Duration.ofMillis(pollInterval))
                .onOverflow().drop()
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .flatMap(t -> queue.dequeue(chunkSize).convert().toPublisher())
                .flatMap(t -> Multi.createFrom().iterable(t))
                .flatMap(m -> processor.process(m)
                        .onFailure().recoverWithUni(t -> onError(m))
                        .convert().toPublisher())
                .flatMap(transaction -> queue.saveTransaction(transaction).emitOn(Infrastructure.getDefaultWorkerPool()).convert().toPublisher())
                .onFailure().recoverWithMulti(e -> {
                    log.error("Error processing payment message", e);
                    return Multi.createFrom().empty();
                })
                .subscribe()
                .with(
                        r -> {
                            log.info("Processed transaction: " + r);
                        },
                        failure -> log.error("Failed to process transaction", failure),
                        () -> log.info("All transactions processed")
                );
    }

    private Uni<PaymentTransaction> onError(PaymentMessage m) {
        return queue.enqueue(m)
                .onItem().invoke(r -> log.warn("ReEnque payment message: " + m.getCorrelationId()))
                .map(l -> new PaymentTransaction(0, "0", OffsetDateTime.now(), m.getCorrelationId()));
    }
}
