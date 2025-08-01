package com.dolphs.payment.repository;

import com.dolphs.payment.domain.model.PaymentTransaction;
import com.dolphs.payment.domain.model.Summary;
import com.hazelcast.aggregation.Aggregator;

import java.io.Serializable;
import java.util.Map;

public class TransactionAggregator implements Aggregator<Map.Entry<String, PaymentTransaction>, Summary> , Serializable {
    double sum;
    int totalRequests;
    int processorId;
    private static final long serialVersionUID = -8885817712041252438L;

    public TransactionAggregator() {
    }

    public TransactionAggregator(int processorId) {
        this.processorId = processorId;
    }

    @Override
    public void accumulate(Map.Entry<String, PaymentTransaction> entry) {
        sum += entry.getValue().getAmount();
        totalRequests += 1;
    }

    @Override
    public void combine(Aggregator aggregator) {
        sum += TransactionAggregator.class.cast(aggregator).sum;
        totalRequests += TransactionAggregator.class.cast(aggregator).totalRequests;
    }

    @Override
    public Summary aggregate() {
        return new Summary(totalRequests, sum, processorId);
    }

}

