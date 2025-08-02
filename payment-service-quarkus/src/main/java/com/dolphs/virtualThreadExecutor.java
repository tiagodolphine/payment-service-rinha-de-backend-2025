package com.dolphs;

import io.quarkus.virtual.threads.VirtualThreads;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.concurrent.ExecutorService;

@ApplicationScoped
public class virtualThreadExecutor {

    @Inject
    @VirtualThreads
    ExecutorService executorService;

    public void fireAndForget(Runnable runnable) {
        executorService.execute(runnable);
    }
}
