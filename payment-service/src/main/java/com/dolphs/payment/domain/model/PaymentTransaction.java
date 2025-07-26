package com.dolphs.payment.domain.model;

import java.time.OffsetDateTime;

public class PaymentTransaction {
    private double amount;
    private int processorId;
    private long messageId;
    private OffsetDateTime timestamp;
    private long id;

    public PaymentTransaction(double amount, int processorId, OffsetDateTime timestamp, long messageId) {
        this.amount = amount;
        this.processorId = processorId;
        this.timestamp = timestamp;
        this.messageId = messageId;
    }

    public double getAmount() {
        return amount;
    }

    public int getProcessorId() {
        return processorId;
    }

    public OffsetDateTime getTimestamp() {
        return timestamp;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setProcessorId(int processorId) {
        this.processorId = processorId;
    }

    public void setTimestamp(OffsetDateTime timestamp) {
        this.timestamp = timestamp;
    }
}