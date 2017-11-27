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

public class PerfLoggerTest {
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

    //test for logger set at TRACE
    @Test
    public void logAtTraceSimpleStart() {
        setupTraceLogger();

        long start = perfLogger.start();
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(1, false, true);
        verifyDebugInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtTraceMessageStart() {
        setupTraceLogger();

        long start = perfLogger.start("Start message");
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(2, true, true);
        verifyDebugInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtTraceSimpleStartWithInfoLog() {
        setupTraceLogger();

        long start = perfLogger.startForInfoLog();
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(1, false, true);
        verifyInfoInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtTraceMessageStartWithInfoLog() {
        setupTraceLogger();

        long start = perfLogger.startForInfoLog("Start message");
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(2, true, true);
        verifyInfoInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }
    //end TRACE tests

    //test for logger set at DEBUG
    @Test
    public void logAtDebugSimpleStart() {
        setupDebugLogger();

        long start = perfLogger.start();
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(1, false, false);
        verifyDebugInteractions(3, true);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtDebugMessageStart() {
        setupDebugLogger();

        long start = perfLogger.start("Start message");
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(2, false, false);
        verifyDebugInteractions(3, true);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtDebugSimpleStartWithInfoLog() {
        setupDebugLogger();

        long start = perfLogger.startForInfoLog();
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(1, false, false);
        verifyDebugInteractions(1, true);
        verifyInfoInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtDebugMessageStartWithInfoLog() {
        setupDebugLogger();

        long start = perfLogger.startForInfoLog("Start message");
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(2, false, false);
        verifyDebugInteractions(1, true);
        verifyInfoInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }
    //end DEBUG tests

    //test for logger set at INFO
    @Test
    public void logAtInfoSimpleStart() {
        setupInfoLogger();

        long start = perfLogger.start();
        perfLogger.end(start, -1, "message", "argument");

        verifyDebugInteractions(1, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtInfoMessageStart() {
        setupInfoLogger();

        long start = perfLogger.start("Start message");
        perfLogger.end(start, -1, "message", "argument");

        verifyDebugInteractions(1, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtInfoSimpleStartWithInfoLog() {
        setupInfoLogger();

        long start = perfLogger.startForInfoLog();
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(1, false, false);
        verifyDebugInteractions(1, false);
        verifyInfoInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logAtInfoMessageStartWithInfoLog() {
        setupInfoLogger();

        long start = perfLogger.startForInfoLog("Start message");
        perfLogger.end(start, -1, "message", "argument");

        verifyTraceInteractions(2, false, false);
        verifyDebugInteractions(1, false);
        verifyInfoInteractions(2, false);
        verifyNoMoreInteractions(logger);
    }
    //end INFO tests

    private void setupTraceLogger() {
        when(logger.isTraceEnabled()).thenReturn(true);
        setupDebugLogger();
    }
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
