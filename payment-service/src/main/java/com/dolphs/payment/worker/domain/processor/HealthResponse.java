package com.dolphs.payment.worker.domain.processor;

class HealthResponse {
    boolean failing;
    long minResponseTime;

    public boolean isFailing() {
        return failing;
    }

    public void setFailing(boolean failing) {
        this.failing = failing;
    }

    public long getMinResponseTime() {
        return minResponseTime;
    }

    public void setMinResponseTime(long minResponseTime) {
        this.minResponseTime = minResponseTime;
    }

    public boolean isHealthy() {
        return !failing;
    }
}
