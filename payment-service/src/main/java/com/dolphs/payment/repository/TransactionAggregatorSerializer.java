package com.dolphs.payment.repository;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public class TransactionAggregatorSerializer implements CompactSerializer<TransactionAggregator> {

    @Override
    public TransactionAggregator read(CompactReader reader) {
        double sum = reader.readFloat64("sum");
        int totalRequests = reader.readInt32("totalRequests");
        int processorId = reader.readInt32("processorId");
        TransactionAggregator aggregator = new TransactionAggregator(processorId);
        aggregator.sum = sum;
        aggregator.totalRequests = totalRequests;
        return aggregator;
    }

    @Override
    public void write(CompactWriter writer, TransactionAggregator aggregator) {
        writer.writeFloat64("sum", aggregator.sum);
        writer.writeInt32("totalRequests", aggregator.totalRequests);
        writer.writeInt32("processorId", aggregator.processorId);
    }

    @Override
    public String getTypeName() {
        return "Aggregator";
    }

    @Override
    public Class<TransactionAggregator> getCompactClass() {
        return TransactionAggregator.class;
    }
}