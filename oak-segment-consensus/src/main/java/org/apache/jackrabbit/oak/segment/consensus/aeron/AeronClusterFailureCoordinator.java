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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentTerminationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AeronClusterFailureCoordinator {

    private static final Logger log = LoggerFactory.getLogger(AeronClusterFailureCoordinator.class);

    private static final long FATAL_SHUTDOWN_DELAY_MS = 2000L;
    private static final long STARTUP_RESET_DELAY_MS = 10000L;

    private final CrashHandler crashHandler;
    private final Executor executor;
    private final AtomicBoolean shutdownRequested;
    private final AtomicBoolean crashRecorded = new AtomicBoolean();
    private final Sleeper sleeper;
    private final Runnable shutdownAction;
    private final Supplier<Runnable> shutdownCallbackSupplier;

    static AeronClusterFailureCoordinator system(CrashHandler crashHandler,
                                                 Executor executor,
                                                 AtomicBoolean shutdownRequested,
                                                 Runnable shutdownAction,
                                                 Supplier<Runnable> shutdownCallbackSupplier) {
        return new AeronClusterFailureCoordinator(
            crashHandler,
            executor,
            shutdownRequested,
            Thread::sleep,
            shutdownAction,
            shutdownCallbackSupplier
        );
    }

    AeronClusterFailureCoordinator(CrashHandler crashHandler,
                                   Executor executor,
                                   AtomicBoolean shutdownRequested,
                                   Sleeper sleeper,
                                   Runnable shutdownAction,
                                   Supplier<Runnable> shutdownCallbackSupplier) {
        this.crashHandler = crashHandler;
        this.executor = executor;
        this.shutdownRequested = shutdownRequested;
        this.sleeper = sleeper;
        this.shutdownAction = shutdownAction;
        this.shutdownCallbackSupplier = shutdownCallbackSupplier;
    }

    ErrorHandler decorate(ErrorHandler handler) {
        return throwable -> {
            handler.onError(throwable);
            maybeScheduleFatalShutdown(throwable);
        };
    }

    /**
     * Aeron terminated the consensus module or service container (termination hook, run on its agent
     * thread): the node can no longer apply the log, so it stops serving through the same path as a
     * FATAL error, minus the crash record.
     */
    void onAeronTermination(String component) {
        if (!requestShutdown()) {
            return;
        }
        log.error("🛑 Aeron {} terminated - shutting the node down", component);
        executor.execute(this::shutdownAndNotify);
    }

    boolean requestShutdown() {
        return shutdownRequested.compareAndSet(false, true);
    }

    void scheduleSuccessfulStartupReset() {
        executor.execute(() -> {
            try {
                sleeper.sleep(STARTUP_RESET_DELAY_MS);
                if (crashHandler != null) {
                    crashHandler.reset();
                    log.info("✅ Startup successful - crash markers reset");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("Failed to reset crash markers", e);
            }
        });
    }

    private void maybeScheduleFatalShutdown(Throwable throwable) {
        // The clustered service stops its agent with an AgentTerminationException that carries the FATAL cause.
        Throwable error = throwable instanceof AgentTerminationException && throwable.getCause() instanceof AeronException
            ? throwable.getCause()
            : throwable;
        if (!(error instanceof AeronException) || crashHandler == null) {
            return;
        }

        AeronException ex = (AeronException) error;
        if (!crashHandler.shouldStop(ex)) {
            return;
        }

        log.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.error("🚨 FATAL Aeron error detected - scheduling graceful shutdown");
        log.error("   Error: {} ({})", ex.getClass().getSimpleName(), ex.getMessage());
        log.error("   Category: {}", ex.category());
        log.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Record the crash even when Aeron's termination hook already started the shutdown for this
        // same failure (the hook runs before the agent's error handler), so crash-loop state is kept.
        if (crashRecorded.compareAndSet(false, true)) {
            crashHandler.handleCrash(ex);
            log.warn("📛 Crash state: {}", crashHandler.getState());
        }
        if (!requestShutdown()) {
            return;
        }

        executor.execute(() -> {
            try {
                log.info("⏳ Waiting 2 seconds before shutdown to allow error logging...");
                sleeper.sleep(FATAL_SHUTDOWN_DELAY_MS);

                log.info("🛑 Initiating graceful shutdown due to FATAL error...");
                shutdownAndNotify();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void shutdownAndNotify() {
        try {
                shutdownAction.run();

                Runnable shutdownCallback = shutdownCallbackSupplier.get();
                if (shutdownCallback != null) {
                    log.info("📞 Invoking shutdown callback...");
                    shutdownCallback.run();
                } else {
                    log.warn("⚠️  No shutdown callback set - process will continue running");
                    log.warn("   Set shutdown callback to exit JVM or restart container");
                }
            } catch (Exception e) {
                log.error("Error during shutdown", e);
            }
    }

    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }
}
