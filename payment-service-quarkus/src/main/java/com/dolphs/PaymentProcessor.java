package com.dolphs;

import com.dolphs.model.HealthResponse;
import com.dolphs.model.PaymentMessage;
import com.dolphs.model.PaymentTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class PaymentProcessor {

    public static final Duration TIMEOUT = Duration.ofMillis(1500);
    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);

    @ConfigProperty(name = "processor.retry.max")
    private int maxRetries;
    private AtomicInteger retries = new AtomicInteger(0);
    private AtomicReference<Client> client;
    private Client paymentProcessorDefault;
    private Client paymentProcessorFallback;

    private final AtomicBoolean cachedHealthCheck = new AtomicBoolean(true);
    private final AtomicReference<Duration> wait = new AtomicReference<>(Duration.ZERO);

    private class Client {
        private final WebClient webClient;
        private final String id;
        private AtomicInteger retry = new AtomicInteger(0);

        public Client(WebClient webClient, String id) {
            this.webClient = webClient;
            this.id = id;
        }
    }

    public PaymentProcessor(@ConfigProperty(name = "payment-processor.fallback.url")
                            String paymentProcessorFallbackUrl,
                            @ConfigProperty(name = "payment-processor.default.url")
                            String paymentProcessorDefaultUrl,
                            @ConfigProperty(name = "payment-processor.default.port")
                            String paymentProcessorDefaultPort,
                            @ConfigProperty(name = "payment-processor.fallback.port")
                            String paymentProcessorFallbackPort,
                            Vertx vertx) {

        this.paymentProcessorDefault = new Client(
                WebClient.create(vertx,
                        new WebClientOptions()
                                .setDefaultHost(paymentProcessorDefaultUrl)
                                .setMaxPoolSize(2000)
                                .setDefaultPort(Integer.valueOf(paymentProcessorDefaultPort))), "default");

        this.paymentProcessorFallback = new Client(WebClient.create(vertx,
                new WebClientOptions()
                        .setMaxPoolSize(2000)
                        .setDefaultHost(paymentProcessorFallbackUrl)
                        .setDefaultPort(Integer.valueOf(paymentProcessorFallbackPort))), "fallback");
        this.client = new AtomicReference<>(paymentProcessorDefault);
    }


    public void switchFallbackClient(boolean fallback) {
        if (maxRetries > retries.incrementAndGet()) {
            return;
        }
        wait.set(fallback ? Duration.ofMillis(1000) : Duration.ZERO);
        cachedHealthCheck.set(false);
    }

    @PostConstruct
    public void startHealthCheckRefresh() {
        Multi.createFrom().ticks().every(Duration.ofSeconds(3))
                .onItem().transform(tick -> {
                    //log.info("Starting health check refresh");
                    return refreshHealthCheck();
                })
                .subscribe();
    }

    private Uni<Void> refreshHealthCheck() {
        //log.info("Health check refresh completed {}", result);
        return callRealHealthCheck()
                .onItem().invoke(r -> {
                    if (r) {
                        switchDefaultClient();
                    } else {
                        switchFallbackClient(false);
                    }
                })
                .replaceWithVoid();
    }

    private Uni<Boolean> callRealHealthCheck() {
        return paymentProcessorDefault.webClient
                .get("/payments/service-health")
                .timeout(Duration.ofSeconds(5).toMillis())
                .send()
                .map(res -> {
                    if (res.statusCode() != 200) {
                        log.error("Health check failed with status code: {}", res.statusCode());
                        return false;
                    }
                    HealthResponse healthResponse = res.bodyAsJson(HealthResponse.class);
                    return healthResponse.isHealthy();
                })
                .onFailure().recoverWithItem(throwable -> false);
    }

    public Boolean healthCheck() {
        return cachedHealthCheck.get();
    }

    public void switchDefaultClient() {
        cachedHealthCheck.set(true);
        wait.set(Duration.ZERO);
        retries.set(0);
    }

    public Uni<PaymentTransaction> process(PaymentMessage payment) {
        //POST /payments
        var currentClient = resolveClient();
        return Uni.createFrom().item(currentClient)
                .onItem().transformToUni(c -> {
                    if (wait.get() == Duration.ZERO) {
                        return Uni.createFrom().item(c);
                    } else {
                        return Uni.createFrom().item(c).onItem().delayIt().by(wait.get());
                    }
                })
                .flatMap(c -> {
                    OffsetDateTime now = OffsetDateTime.now();
                    return c.webClient
                            .post("/payments")
                            .timeout(TIMEOUT.toMillis())
                            .putHeader("Content-Type", "application/json")
                            .sendJsonObject(JsonObject.of(
                                    "amount", payment.getAmount(),
                                    "correlationId", payment.getCorrelationId(),
                                    "requestedAt", now.toString()))
                            .map(res -> {
                                if (res.statusCode() > 400 && res.statusCode() < 499) {
                                    log.error("Error processing payment: {}", res.statusCode());
                                    return new PaymentTransaction(payment.getAmount(), currentClient.id, now, payment.getCorrelationId());
                                    //throw new IllegalArgumentException("error " + res.statusCode());
                                } else if (res.statusCode() > 300) {
                                    throw new RuntimeException("Error processing payment");
                                }
                                return new PaymentTransaction(payment.getAmount(), currentClient.id, now, payment.getCorrelationId());
                            })
                            .onFailure().invoke(e -> {
                                switchFallbackClient(currentClient == paymentProcessorFallback);
                                log.error("Failed to process payment", e);
                                // handle/log error, return fallback or propagate
                            });
                })
                .onItem().invoke(i -> switchDefaultClient());

    }

    private Client resolveClient() {
        return healthCheck() ? paymentProcessorDefault : paymentProcessorFallback;
    }
}
