package com.dolphs.payment.rest;

import com.dolphs.payment.domain.model.PaymentMessage;
import com.dolphs.payment.domain.model.PaymentSummary;
import com.dolphs.payment.domain.model.Summary;
import com.dolphs.payment.domain.service.PaymentService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;

@Configuration(proxyBeanMethods = false)
public class RouterEndpoints {

    private final PaymentService paymentService;

    public RouterEndpoints(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Register Afterburner for bytecode speedups
        mapper.registerModule(new AfterburnerModule());
        // Disable features that slow down serialization
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(SerializationFeature.INDENT_OUTPUT);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    @Bean
    public RouterFunction<ServerResponse> route() {
        return RouterFunctions
                .route(POST("/payments").and(RequestPredicates.accept(MediaType.APPLICATION_JSON)), this::createPayment)
                .andRoute(GET("/payments-summary"), this::getPaymentSummary);
    }

    @RegisterReflectionForBinding(PaymentMessage.class)
    public Mono<ServerResponse> createPayment(ServerRequest request) {
        return Mono.just(request)
                .flatMap(r -> r.bodyToMono(PaymentMessage.class)
                        .map(paymentMessage -> {
                            boolean generate = r.queryParam("generate").map(Boolean::parseBoolean).orElse(false);
                            if (generate) paymentMessage.setCorrelationId(UUID.randomUUID().toString());
                            return paymentMessage;
                        })
                        .doOnNext(p -> paymentService.createPayment(p).subscribeOn(Schedulers.boundedElastic()).subscribe()))
                .then(ServerResponse.ok().build());
    }

    @RegisterReflectionForBinding({PaymentSummary.class, Summary.class})
    public Mono<ServerResponse> getPaymentSummary(ServerRequest request) {
        Optional<OffsetDateTime> from = request.queryParam("from").map(OffsetDateTime::parse);
        Optional<OffsetDateTime> to = request.queryParam("to").map(OffsetDateTime::parse);
        return paymentService.getPaymentSummary(from, to)
                .flatMap(summary -> ServerResponse.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(BodyInserters.fromValue(summary)))
                .switchIfEmpty(ServerResponse.ok().build());
    }
}
