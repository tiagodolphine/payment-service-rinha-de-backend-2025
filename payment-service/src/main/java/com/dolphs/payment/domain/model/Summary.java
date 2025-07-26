package com.dolphs.payment.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Summary {
    private int totalRequests;
    private double totalAmount;
    @JsonIgnore
    private int processorId;

    public Summary(int totalRequests, double totalAmount, int processorId) {
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

    public int getProcessorId() {
        return processorId;
    }

    public void setProcessorId(int processorId) {
        this.processorId = processorId;
    }
}
