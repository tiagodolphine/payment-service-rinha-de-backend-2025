package com.dolphs.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class Summary {
    private int totalRequests;
    private double totalAmount;
    @JsonIgnore
    private String processorId;

    public Summary(int totalRequests, double totalAmount, String processorId) {
        this.totalRequests = totalRequests;
        this.totalAmount = totalAmount;
        this.processorId = processorId;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public void setTotalRequests(int totalRequests) {
        this.totalRequests = totalRequests;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }
}
