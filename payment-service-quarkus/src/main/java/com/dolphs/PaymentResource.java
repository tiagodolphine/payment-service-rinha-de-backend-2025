package com.dolphs;

import com.dolphs.model.PaymentMessage;
import com.dolphs.model.PaymentSummary;
import io.quarkus.vertx.web.Body;
import io.quarkus.vertx.web.Param;
import io.quarkus.vertx.web.Route;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class PaymentResource {

    @Inject
    PaymentQueue paymentQueue;

    @Inject
    virtualThreadExecutor executor;

    @Route(path = "/payments", produces = "application/json", methods = Route.HttpMethod.POST)
    Uni<Void> addPayment(@Body PaymentMessage message, @Param Optional<Boolean> generate) {
        executor.fireAndForget(() -> {
            if (generate.orElse(false)) {
                message.setCorrelationId(UUID.randomUUID().toString());
            }
            paymentQueue.enqueue(message);
        });
        return Uni.createFrom().voidItem();
    }

    @Route(path = "/payments-summary", produces = "application/json", methods = Route.HttpMethod.GET)
    Uni<PaymentSummary> summary(@Param Optional<String> from, @Param Optional<String> to) {
        return paymentQueue.getSummary(
                from.map(OffsetDateTime::parse).orElse(OffsetDateTime.now().minusHours(1)),
                to.map(OffsetDateTime::parse).orElse(OffsetDateTime.now()));
    }

    @Route(path = "/health", methods = Route.HttpMethod.GET)
    Uni<Void> health() {
        return Uni.createFrom().voidItem();
    }
}
