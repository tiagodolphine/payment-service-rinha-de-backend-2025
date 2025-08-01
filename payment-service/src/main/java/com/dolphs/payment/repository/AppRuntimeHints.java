package com.dolphs.payment.repository;

import com.hazelcast.aggregation.Aggregator;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

public class AppRuntimeHints implements RuntimeHintsRegistrar {

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        // Register serialization
        hints.serialization().registerType(TransactionAggregator.class);
        hints.serialization().registerType(PaymentRepositoryImpl.class);
        hints.serialization().registerType(Aggregator.class);
    }
}