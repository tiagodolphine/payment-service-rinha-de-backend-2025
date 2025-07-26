package com.dolphs.payment.worker.domain.processor;

import com.dolphs.payment.domain.model.PaymentMessage;
import com.dolphs.payment.domain.model.PaymentTransaction;
import com.dolphs.payment.repository.PaymentRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);
    private final PaymentRepositoryImpl paymentTransactionRepository;
    private final PaymentProcessorClient paymentProcessorClient;

    public PaymentProcessor(PaymentRepositoryImpl paymentTransactionRepository, PaymentProcessorClient paymentProcessorClient) {
        this.paymentTransactionRepository = paymentTransactionRepository;
        this.paymentProcessorClient = paymentProcessorClient;
    }

    public Mono<PaymentTransaction> process(PaymentMessage paymentMessage) {
        return paymentProcessorClient.process(paymentMessage);
    }

    private Mono<Void> onError(PaymentMessage payment) {
        return Mono.empty();
    }
}
