package com.dolphs.payment.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;

public class PaymentMessage {
    private double amount;
    private String correlationId;
    @JsonIgnore
    private long id;

    public PaymentMessage() {
    }

    public PaymentMessage(double amount, String correlationId) {
        this.amount = amount;
        this.correlationId = correlationId;
    }

    public PaymentMessage(double amount, String correlationId, long id) {
        this.amount = amount;
        this.correlationId = correlationId;
        this.id = id;
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

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}