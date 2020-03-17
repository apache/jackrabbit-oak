/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.commons.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Future} that accepts completion listener. The listener is invoked
 * once the future's computation is {@linkplain Future#isDone() complete}.
 * If the computation has already completed when the listener is added, the
 * listener will execute immediately.
 *
 * <p>Listener is invoked synchronously on the same thread which is used to
 * executed the Future</p>
 */
public class NotifyingFutureTask extends FutureTask<Void> {
    private final AtomicBoolean completed = new AtomicBoolean(false);

    private volatile Runnable onComplete;

    public NotifyingFutureTask(Callable<Void> callable) {
        super(callable);
    }

    public NotifyingFutureTask(Runnable task) {
        super(task, null);
    }

    /**
     * Set the on complete handler. The handler will run exactly once after
     * the task terminated. If the task has already terminated at the time of
     * this method call the handler will execute immediately.
     * <p>
     * Note: there is no guarantee to which handler will run when the method
     * is called multiple times with different arguments.
     * </p>
     * @param onComplete listener to invoke upon completion
     */
    public void onComplete(Runnable onComplete) {
        this.onComplete = onComplete;
        if (isDone()) {
            run(onComplete);
        }
    }

    @Override
    protected void done() {
        run(onComplete);
    }

    private void run(Runnable onComplete) {
        if (onComplete != null && completed.compareAndSet(false, true)) {
            onComplete.run();
        }
    }

    private static final Runnable NOP = new Runnable() {
        @Override
        public void run() {
        }
    };

    public static NotifyingFutureTask completed() {
        NotifyingFutureTask f = new NotifyingFutureTask(NOP);
        f.run();
        return f;
    }
}
