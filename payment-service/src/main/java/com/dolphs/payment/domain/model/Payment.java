package com.dolphs.payment.domain.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;

import java.time.OffsetDateTime;

@RegisterReflectionForBinding({OffsetDateTime.class, Payment.class})
public class Payment {
    private double amount;
    private String correlationId;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private OffsetDateTime requestedAt;

    public Payment() {
    }

    public Payment(double amount, String correlationId, OffsetDateTime requestedAt) {
        this.amount = amount;
        this.correlationId = correlationId;
        this.requestedAt = requestedAt;
    }

    public double getAmount() {
        return amount;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public OffsetDateTime getRequestedAt() {
        return requestedAt;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public void setRequestedAt(OffsetDateTime requestedAt) {
        this.requestedAt = requestedAt;
    }
}