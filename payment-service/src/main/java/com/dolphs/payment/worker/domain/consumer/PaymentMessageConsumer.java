package com.dolphs.payment.worker.domain.consumer;

import com.dolphs.payment.domain.model.PaymentMessage;
import com.dolphs.payment.domain.model.PaymentTransaction;
import com.dolphs.payment.repository.PaymentRepositoryImpl;
import com.dolphs.payment.worker.domain.processor.PaymentProcessor;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class PaymentMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentMessageConsumer.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean skip = new AtomicBoolean(false);
    private final PaymentProcessor paymentProcessor;
    private final PaymentRepositoryImpl paymentRepository;
    private final int chunkSize;
    private Disposable loop;

    public PaymentMessageConsumer(PaymentProcessor paymentProcessor, PaymentRepositoryImpl paymentRepository,
                                  @Value("${chunkSize}") int chunkSize) {
        this.chunkSize = chunkSize;
        this.paymentProcessor = paymentProcessor;
        this.paymentRepository = paymentRepository;
        try {
            loop = fetchMessages()
                    .subscribeOn(Schedulers.boundedElastic())
                    .repeatWhen(completed -> completed.delayElements(Duration.ofMillis(5)))
                    .subscribe();
        } catch (Exception e) {
            log.error("Error in message consumer loop", e);
        }
    }

    @PreDestroy
    public void onShutdown() {
        log.info("Spring context shutting down, stopping consumer...");
        running.set(false);
        loop.dispose();
    }

    public Mono<Void> fetchMessages() {
        return paymentRepository.processChunk(chunkSize, this::onMessage);
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