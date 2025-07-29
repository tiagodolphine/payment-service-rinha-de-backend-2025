package com.dolphs.payment.worker.domain.processor;

import com.dolphs.payment.domain.model.Payment;
import com.dolphs.payment.domain.model.PaymentMessage;
import com.dolphs.payment.domain.model.PaymentTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class PaymentProcessorClient {

    public static final Duration TIMEOUT = Duration.ofMillis(5000);
    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorClient.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${processor.retry.max}")
    private int maxRetries;


    private AtomicReference<Client> client;
    private Client paymentProcessorDefault;
    private Client paymentProcessorFallback;
    private final AtomicBoolean cachedHealthCheck = new AtomicBoolean(true);

    private class Client {
        private final WebClient webClient;
        private final int id;
        private AtomicInteger retry = new AtomicInteger(0);

        public Client(WebClient webClient, int id) {
            this.webClient = webClient;
            this.id = id;
        }
    }

    public PaymentProcessorClient(@Value("${payment-processor.fallback.url}")
                                  String paymentProcessorFallbackUrl,
                                  @Value("${payment-processor.default.url}")
                                  String paymentProcessorDefaultUrl) {
        ConnectionProvider connectionProvider = ConnectionProvider.builder("myConnectionPool")
                .maxConnections(2000)
                .pendingAcquireMaxCount(2000)
                .maxIdleTime(Duration.ofSeconds(30))
                .maxLifeTime(Duration.ofSeconds(5))
                .evictInBackground(Duration.ofSeconds(10))
                .build();

        ConnectionProvider connectionProvider2 = ConnectionProvider.builder("myConnectionPool2")
                .maxConnections(2000)
                .pendingAcquireMaxCount(2000)
                .maxIdleTime(Duration.ofSeconds(30))
                .maxLifeTime(Duration.ofSeconds(5))
                .evictInBackground(Duration.ofSeconds(10))
                .build();

        this.paymentProcessorDefault = new Client(WebClient.builder()
                .baseUrl(paymentProcessorDefaultUrl)
                .clientConnector(new ReactorClientHttpConnector(
                        HttpClient.create(connectionProvider).responseTimeout(TIMEOUT)
                ))
                .build(), 1);
        this.paymentProcessorFallback = new Client(WebClient.builder()
                .baseUrl(paymentProcessorFallbackUrl)
                .clientConnector(new ReactorClientHttpConnector(
                        HttpClient.create(connectionProvider2).responseTimeout(TIMEOUT)
                ))
                .build(), 2);
        this.client = new AtomicReference<>(paymentProcessorDefault);
    }


    public void switchFallbackClient() {
        cachedHealthCheck.set(true);
//        if (this.client.get().retry.getAndIncrement() > maxRetries) {
//            this.client.set(paymentProcessorFallback);
//        }
    }

    @PostConstruct
    public void startHealthCheckRefresh() {
        Flux.interval(Duration.ZERO, Duration.ofSeconds(5))
                .flatMap(tick -> {
                    //log.info("Starting health check refresh");
                    return refreshHealthCheck();
                })
                .subscribe();
    }

    private Mono<Void> refreshHealthCheck() {
        return callRealHealthCheck()
                .doOnNext(result -> {
                    //log.info("Health check refresh completed {}", result);
                    cachedHealthCheck.set(result);
                })
                .then();
    }

    @RegisterReflectionForBinding(HealthResponse.class)
    private Mono<Boolean> callRealHealthCheck() {
        return paymentProcessorDefault.webClient
                .get()
                .uri("/payments/service-health")
                .retrieve()
                .bodyToMono(HealthResponse.class)
                .map(HealthResponse::isHealthy)
                .onErrorReturn(false);
    }

    public Boolean healthCheck() {
        return cachedHealthCheck.get();
    }

    public void switchDefaultClient() {
        cachedHealthCheck.set(true);
        //this.paymentProcessorFallback.retry.set(0);
        //this.client.set(paymentProcessorDefault);
    }

    @RegisterReflectionForBinding(Payment.class)
    public Mono<PaymentTransaction> process(PaymentMessage payment) {
        //POST /payments
        var currentClient = resolveClient();
        var value = Mono.just(new Payment(payment.getAmount(), payment.getCorrelationId(), OffsetDateTime.now()));
        return Mono.just(currentClient)
                .flatMap(c -> c.webClient
                        .post()
                        .uri("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(value, Payment.class)
                        .retrieve()
                        .onStatus(HttpStatusCode::is4xxClientError, response -> {
                            log.error("Error processing payment: {}", response.statusCode());
                            return Mono.error(new IllegalArgumentException("error " + response.statusCode()));
                        })
                        .toBodilessEntity()
                        .flatMap(r -> value)
                        .map(r -> new PaymentTransaction(r.getAmount(), c.id, r.getRequestedAt(), payment.getId()))
                        .onErrorReturn(IllegalArgumentException.class, new PaymentTransaction(payment.getAmount(), -1, OffsetDateTime.now(), payment.getId()))
                        .onErrorResume(e -> {
                            switchFallbackClient();
                            log.error("Failed to process payment", e);
                            // handle/log error, return fallback or propagate
                            return Mono.error(e);
                        })
                        .doOnSuccess(t -> switchDefaultClient())
                );
    }

    private Client resolveClient() {
        return healthCheck() ? paymentProcessorDefault : paymentProcessorFallback;
    }
}