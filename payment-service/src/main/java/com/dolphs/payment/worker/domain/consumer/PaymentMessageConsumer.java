package com.dolphs.payment.worker.domain.consumer;

import com.dolphs.payment.domain.model.PaymentMessage;
import com.dolphs.payment.domain.model.PaymentTransaction;
import com.dolphs.payment.repository.PaymentRepositoryImpl;
import com.dolphs.payment.worker.domain.processor.PaymentProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class PaymentMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentMessageConsumer.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final PaymentProcessor paymentProcessor;
    private final PaymentRepositoryImpl paymentRepository;
    private final int chunkSize;
    private Disposable loop;

    public PaymentMessageConsumer(PaymentProcessor paymentProcessor, PaymentRepositoryImpl paymentRepository,
                                  @Value("${chunkSize}") int chunkSize) {
        this.chunkSize = chunkSize;
        this.paymentProcessor = paymentProcessor;
        this.paymentRepository = paymentRepository;
    }

    @PreDestroy
    public void onShutdown() {
        if (loop != null && !loop.isDisposed()) {
            log.info("Stopping PaymentMessageConsumer loop");
            loop.dispose();
        }
        running.set(false);
        log.info("PaymentMessageConsumer stopped");
    }

    @PostConstruct
    public void processMessages() {
        running.set(true);
        loop = paymentRepository.processChunk(chunkSize, this::onMessage)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

    public Mono<PaymentMessage> onMessageRemote(PaymentMessage paymentMessage) {
        return Mono.empty();
    }

    public Mono<PaymentTransaction> onMessage(PaymentMessage paymentMessage) {
        //log.info("Received PaymentMessage {}", paymentMessage);
        return paymentProcessor.process(paymentMessage)
                .doOnError(e -> {
                    log.error("Error processing payment message: {}", paymentMessage, e);
                });
    }
}