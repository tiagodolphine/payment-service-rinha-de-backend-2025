package com.dolphs.payment.repository;

import com.dolphs.payment.domain.model.PaymentTransaction;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import java.time.OffsetDateTime;

public class TransactionSerializer implements CompactSerializer<PaymentTransaction> {

    @Override
    public PaymentTransaction read(CompactReader reader) {
        double amount = reader.readFloat64("amount");
        int processorId = reader.readInt32("processorId");
        String timestampStr = reader.readString("timestamp");
        String id = reader.readString("id");

        OffsetDateTime timestamp = OffsetDateTime.parse(timestampStr);

        PaymentTransaction transaction = new PaymentTransaction(amount, processorId, timestamp, id);
        return transaction;
    }

    @Override
    public void write(CompactWriter writer, PaymentTransaction transaction) {
        writer.writeFloat64("amount", transaction.getAmount());
        writer.writeInt32("processorId", transaction.getProcessorId());
        writer.writeString("timestamp", transaction.getTimestamp().toString());
        writer.writeString("id", transaction.getId());
    }

    @Override
    public String getTypeName() {
        return "PaymentTransaction";
    }

    @Override
    public Class<PaymentTransaction> getCompactClass() {
        return PaymentTransaction.class;
    }
}