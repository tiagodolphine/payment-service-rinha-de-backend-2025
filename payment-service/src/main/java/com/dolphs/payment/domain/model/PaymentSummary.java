package com.dolphs.payment.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;

@RegisterReflectionForBinding
public class PaymentSummary {

    @JsonProperty(value = "default")
    private Summary defaultSummary;
    private Summary fallback;

    public PaymentSummary(Summary defaultSummary, Summary fallback) {
        this.defaultSummary = defaultSummary;
        this.fallback = fallback;
    }

    public Summary getDefaultSummary() {
        return defaultSummary;
    }

    public void setDefaultSummary(Summary defaultSummary) {
        this.defaultSummary = defaultSummary;
    }

    public Summary getFallback() {
        return fallback;
    }

    public void setFallback(Summary fallback) {
        this.fallback = fallback;
    }

}