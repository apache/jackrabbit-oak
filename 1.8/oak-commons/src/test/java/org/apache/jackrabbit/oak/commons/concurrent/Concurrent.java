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
package org.apache.jackrabbit.oak.commons.concurrent;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A concurrency test tool.
 */
public class Concurrent {

    private Concurrent() {
    }

    /**
     * Run a task concurrently in 2 threads for 1 second.
     *
     * @param message the message
     * @param task the task
     * @throws Exception the first exception that is thrown (if any)
     */
    public static void run(String message, Task task) throws Exception {
        run(message, task, 2, 1000);
    }

    public static void run(String message, final Task task, int threadCount, int millis) throws Exception {
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
        ArrayList<Thread> threads = new ArrayList<Thread>();
        final AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread("Task " + i) {
                @Override
                public void run() {
                    while (!stopped.get()) {
                        try {
                            task.call();
                            counter.incrementAndGet();
                        } catch (Error e) {
                            exception.set(e);
                            stopped.set(true);
                        } catch (Exception e) {
                            exception.set(e);
                            stopped.set(true);
                        }
                    }
                }
            };
            if (threadCount == 1) {
                long stop = millis + System.currentTimeMillis();
                while (System.currentTimeMillis() < stop) {
                    task.call();
                    counter.incrementAndGet();
                }
                millis = 0;
            } else {
                t.start();
                threads.add(t);
            }
        }
        Throwable e = null;
        while (e == null && millis > 0) {
            Thread.sleep(10);
            millis -= 10;
            e = exception.get();
        }
        stopped.set(true);
        for (Thread t : threads) {
            t.join();
        }
        if (e != null) {
            if (e instanceof Exception) {
                throw (Exception) e;
            }
            throw (Error) e;
        }
    }

    public interface Task {
        void call() throws Exception;
    }

}
