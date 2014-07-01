/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment.file;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BackgroundThread extends Thread {

    /** Logger instance */
    private static final Logger log =
            LoggerFactory.getLogger(BackgroundThread.class);

    private final String name;

    private final long interval;

    private long backlog = 0;

    private long lastDuration = 0;

    private long maxDuration = 0;

    BackgroundThread(String name, long interval, Runnable target) {
        super(target, name);

        this.name = name;
        this.interval = interval;

        setDaemon(true);
        setPriority(MIN_PRIORITY);
        start();
    }

    @Override
    public void run() {
        try {
            while (waitUntilNextIteration()) {
                long start = nanoTime();
                super.run();
                long seconds = SECONDS.convert(nanoTime() - start, NANOSECONDS);

                if (lastDuration != seconds) {
                    lastDuration = seconds;
                    if (maxDuration < seconds) {
                        maxDuration = seconds;
                    }
                    // make execution statistics visible in thread dumps
                    setName(name + " " + lastDuration + "/" + maxDuration);
                }
            }
        } catch (InterruptedException e) {
            log.error(name + " interrupted", e);
        }
    }

    void trigger() {
        trigger(false);
    }

    void close() {
        try {
            trigger(true);
            join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error(name + " join interrupted", e);
        }
    }

    private synchronized void trigger(boolean close) {
        if (close) {
            backlog = -1;
        } else if (backlog >= 0) {
            backlog++;
        }
        notify();
    }

    private synchronized boolean waitUntilNextIteration()
            throws InterruptedException {
        if (backlog == 0) {
            // no backlog to process (and not closed), so wait...
            if (interval < 0) {
                wait();
            } else {
                wait(interval);
            }
        }

        if (backlog > 0) {
            backlog--;
        }

        return backlog >= 0;
    }

}
