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
package org.apache.jackrabbit.oak.commons;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Timing tests for {@link PerfLogger} using {@code Thread.sleep} because
 * virtual clock requires currentTimeMillis (OAK-3877)
 */
public class PerfLoggerIT {
    @Mock
    Logger logger;

    private PerfLogger perfLogger;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(logger.isTraceEnabled()).thenReturn(false);
        when(logger.isDebugEnabled()).thenReturn(false);
        when(logger.isInfoEnabled()).thenReturn(false);

        perfLogger = new PerfLogger(logger);
    }

    //test for logger set at DEBUG
    @Test
    public void logAtDebugTimeoutNotHit() {
        setupDebugLogger();

        long start = perfLogger.start();
        perfLogger.end(start, 100, "message", "argument");

        verifyTraceInteractions(1, false, false);
        verifyDebugInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtDebugTimeoutHit() throws InterruptedException {
        setupDebugLogger();

        long start = perfLogger.start();
        Thread.sleep(100);
        perfLogger.end(start, 20, "message", "argument");

        verifyTraceInteractions(1, false, false);
        verifyDebugInteractions(3, true);
        verifyNoMoreInteractions(logger);
    }
    //end DEBUG tests


    //test for logger set at INFO
    @Test
    public void logAtInfoDebugTimeoutHit() throws InterruptedException {
        setupInfoLogger();

        long start = perfLogger.start();
        Thread.sleep(100);
        perfLogger.end(start, 20, "message", "argument");

        verifyDebugInteractions(1, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtInfoInfoTimeoutNotHit() throws InterruptedException {
        setupInfoLogger();

        long start = perfLogger.startForInfoLog();
        Thread.sleep(100);
        perfLogger.end(start, 20, 500, "message", "argument");

        verifyTraceInteractions(1, false, false);
        verifyDebugInteractions(1, false);
        verifyInfoInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtInfoInfoTimeoutHit() throws InterruptedException {
        setupInfoLogger();

        long start = perfLogger.startForInfoLog();
        Thread.sleep(100);
        perfLogger.end(start, 20, 50, "message", "argument");

        verifyTraceInteractions(1, false, false);
        verifyDebugInteractions(1, false);
        verifyInfoInteractions(2, true);
        verifyNoMoreInteractions(logger);
    }
    //end INFO tests

    private void setupDebugLogger() {
        when(logger.isDebugEnabled()).thenReturn(true);
        setupInfoLogger();
    }
    private void setupInfoLogger() {
        when(logger.isInfoEnabled()).thenReturn(true);
    }

    private void verifyTraceInteractions(int enabled, boolean shouldLogStart, boolean shouldLogEnd) {
        verify(logger, times(enabled)).isTraceEnabled();

        if (shouldLogStart) {
            verify(logger, times(1)).trace(anyString());
        }
        if (shouldLogEnd) {
            verify(logger, times(1)).trace(anyString(), any(Object[].class));
        }
    }

    private void verifyDebugInteractions(int enabled, boolean shouldLog) {
        verify(logger, times(enabled)).isDebugEnabled();

        if (shouldLog) {
            verify(logger, times(1)).debug(anyString(), any(Object[].class));
        }
    }

    private void verifyInfoInteractions(int enabled, boolean shouldLog) {
        verify(logger, times(enabled)).isInfoEnabled();

        if (shouldLog) {
            verify(logger, times(1)).info(anyString(), any(Object[].class));
        }
    }
}
