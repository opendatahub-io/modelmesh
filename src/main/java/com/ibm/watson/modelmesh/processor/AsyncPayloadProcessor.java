package com.ibm.watson.modelmesh.processor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AsyncPayloadProcessor implements PayloadProcessor {

    private final PayloadProcessor delegate;

    private final Queue<Payload> payloads = new ConcurrentLinkedQueue<>();

    public AsyncPayloadProcessor(PayloadProcessor delegate) {
        this.delegate = delegate;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleWithFixedDelay(() -> {
            Payload p;
            while ((p = payloads.poll()) != null) {
                delegate.process(p);
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public void process(Payload payload) {
        payloads.add(payload);
    }
}
