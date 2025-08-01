package com.dolphs.model;

import java.time.OffsetDateTime;

public class PaymentTransaction {
    private double amount;
    private String processorId;
    private OffsetDateTime timestamp;
    private String id;

    public PaymentTransaction(double amount, String processorId, OffsetDateTime timestamp, String id) {
        this.amount = amount;
        this.processorId = processorId;
        this.timestamp = timestamp;
        this.id = id;
    }

    public double getAmount() {
        return amount;
    }

    public String getProcessorId() {
        return processorId;
    }

    public OffsetDateTime getTimestamp() {
        return timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    public void setTimestamp(OffsetDateTime timestamp) {
        this.timestamp = timestamp;
    }
}