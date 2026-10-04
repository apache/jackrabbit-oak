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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.junit.Test;

import static io.aeron.exceptions.AeronException.Category.FATAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AeronClusterFailureCoordinatorTest {

    @Test
    public void fatalErrorSchedulesShutdownOnceAndInvokesCallback() {
        CrashHandler crashHandler = mock(CrashHandler.class);
        when(crashHandler.shouldStop(org.mockito.ArgumentMatchers.any(AeronException.class))).thenReturn(true);
        when(crashHandler.getState()).thenReturn("node-crash-1");

        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        AtomicInteger shutdownCalls = new AtomicInteger();
        AtomicInteger callbackCalls = new AtomicInteger();
        List<Long> sleeps = new ArrayList<>();

        AeronClusterFailureCoordinator coordinator = new AeronClusterFailureCoordinator(
            crashHandler,
            Runnable::run,
            shutdownRequested,
            sleeps::add,
            shutdownCalls::incrementAndGet,
            () -> callbackCalls::incrementAndGet
        );

        ErrorHandler handler = coordinator.decorate(throwable -> { });
        handler.onError(new AeronException("boom", FATAL));
        handler.onError(new AeronException("boom-again", FATAL));

        assertTrue(shutdownRequested.get());
        assertEquals(1, shutdownCalls.get());
        assertEquals(1, callbackCalls.get());
        assertEquals(1, sleeps.size());
        assertEquals(2000L, sleeps.get(0).longValue());
        verify(crashHandler).handleCrash(org.mockito.ArgumentMatchers.any(AeronException.class));
    }

    /**
     * A clustered-service agent that stops itself with a FATAL cause (as the service does on a node-local apply
     * failure) reaches the process-exit callback through Agrona's AgentRunner error handling.
     */
    @Test
    public void serviceAgentTerminatedWithAFatalCauseExitsTheProcess() throws Exception {
        CrashHandler crashHandler = mock(CrashHandler.class);
        when(crashHandler.shouldStop(org.mockito.ArgumentMatchers.any(AeronException.class))).thenReturn(true);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicInteger dutyCycles = new AtomicInteger();
        AeronClusterFailureCoordinator coordinator = new AeronClusterFailureCoordinator(
            crashHandler,
            Runnable::run,
            new AtomicBoolean(false),
            millis -> { },
            () -> { },
            () -> exited::countDown
        );
        Agent service = new Agent() {
            @Override
            public int doWork() {
                dutyCycles.incrementAndGet();
                throw new AgentTerminationException("apply failed",
                    new AeronException("node-local apply failure", new java.io.IOException("disk full"), FATAL));
            }

            @Override
            public String roleName() {
                return "clustered-service";
            }
        };
        AgentRunner runner = new AgentRunner(new SleepingMillisIdleStrategy(1), coordinator.decorate(t -> { }),
            null, service);
        try {
            AgentRunner.startOnThread(runner);
            assertTrue(exited.await(5, TimeUnit.SECONDS));
            assertEquals("the agent stops at the failure", 1, dutyCycles.get());
        } finally {
            runner.close();
        }
    }

    @Test
    public void nonFatalErrorDoesNotScheduleShutdown() {
        CrashHandler crashHandler = mock(CrashHandler.class);
        when(crashHandler.shouldStop(org.mockito.ArgumentMatchers.any(AeronException.class))).thenReturn(false);

        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        AtomicInteger shutdownCalls = new AtomicInteger();

        AeronClusterFailureCoordinator coordinator = new AeronClusterFailureCoordinator(
            crashHandler,
            Runnable::run,
            shutdownRequested,
            millis -> { },
            shutdownCalls::incrementAndGet,
            () -> null
        );

        coordinator.decorate(throwable -> { }).onError(new AeronException("warn"));

        assertEquals(0, shutdownCalls.get());
        assertTrue(!shutdownRequested.get());
        verify(crashHandler, never()).handleCrash(org.mockito.ArgumentMatchers.any(AeronException.class));
    }

    @Test
    public void successfulStartupResetUsesDelayAndResetsCrashMarkers() {
        CrashHandler crashHandler = mock(CrashHandler.class);
        List<Long> sleeps = new ArrayList<>();

        AeronClusterFailureCoordinator coordinator = new AeronClusterFailureCoordinator(
            crashHandler,
            Runnable::run,
            new AtomicBoolean(false),
            sleeps::add,
            () -> { },
            () -> null
        );

        coordinator.scheduleSuccessfulStartupReset();

        assertEquals(1, sleeps.size());
        assertEquals(10000L, sleeps.get(0).longValue());
        verify(crashHandler).reset();
    }

    @Test
    public void aeronTerminationRunsTheShutdownPathOnceWithoutRecordingACrash() {
        CrashHandler crashHandler = mock(CrashHandler.class);
        org.agrona.concurrent.ShutdownSignalBarrier barrier = mock(org.agrona.concurrent.ShutdownSignalBarrier.class);
        AtomicInteger callbackCalls = new AtomicInteger();
        AeronClusterFailureCoordinator coordinator = new AeronClusterFailureCoordinator(
            crashHandler,
            Runnable::run,
            new AtomicBoolean(false),
            millis -> { },
            barrier::signal,
            () -> callbackCalls::incrementAndGet
        );

        coordinator.onAeronTermination("Consensus Module");
        coordinator.onAeronTermination("Clustered Service");

        verify(barrier).signal();
        assertEquals(1, callbackCalls.get());
        verify(crashHandler, never()).handleCrash(org.mockito.ArgumentMatchers.any());
    }
}
