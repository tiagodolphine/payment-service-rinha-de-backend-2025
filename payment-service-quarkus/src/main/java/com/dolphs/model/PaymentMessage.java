package com.dolphs.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class PaymentMessage {
    private double amount;
    private String correlationId;

    public PaymentMessage() {
    }

    public PaymentMessage(double amount, String correlationId) {
        this.amount = amount;
        this.correlationId = correlationId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

}