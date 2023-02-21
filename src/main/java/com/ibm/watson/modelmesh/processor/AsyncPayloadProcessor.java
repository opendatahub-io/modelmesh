/*
 * Copyright 2023 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

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
        this(delegate, 1, TimeUnit.MINUTES, Executors.newScheduledThreadPool(1));
    }

    public AsyncPayloadProcessor(PayloadProcessor delegate, int delay, TimeUnit timeUnit,
                                 ScheduledExecutorService executorService) {
        this.delegate = delegate;
        executorService.scheduleWithFixedDelay(() -> {
            Payload p;
            while ((p = payloads.poll()) != null) {
                delegate.process(p);
            }
        }, 0, delay, timeUnit);
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