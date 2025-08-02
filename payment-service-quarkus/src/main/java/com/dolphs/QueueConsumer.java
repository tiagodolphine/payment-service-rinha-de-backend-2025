package com.dolphs;

import com.dolphs.model.PaymentMessage;
import com.dolphs.model.PaymentTransaction;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;

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
    virtualThreadExecutor executor;

    Logger log = LoggerFactory.getLogger(QueueConsumer.class);
    final PaymentTransaction erroTransaction = new PaymentTransaction(0, "0", OffsetDateTime.now(), "Error");

    public void processMessages(@Observes StartupEvent ev) {
        Multi.createFrom().ticks().every(Duration.ofMillis(pollInterval))
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .onOverflow().drop()
                .flatMap(t -> queue.dequeue(chunkSize).convert().toPublisher())
                .flatMap(t -> Multi.createFrom().iterable(t))
                .flatMap(m -> processor.process(m)
                        .onFailure().recoverWithUni(t -> onError(m))
                        .convert().toPublisher())
                .onItem().invoke(transaction ->
                        executor.fireAndForget(() -> {
                            queue.saveTransaction(transaction).await().indefinitely();
                        }))
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
        executor.fireAndForget(() -> {
            queue.enqueue(m).await().indefinitely();
        });
        return Uni.createFrom().item(erroTransaction);
    }
}
