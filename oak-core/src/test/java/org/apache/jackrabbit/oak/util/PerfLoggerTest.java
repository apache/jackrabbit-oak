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
package org.apache.jackrabbit.oak.util;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PerfLoggerTest {

    @Test
    public void testEndDebug() {
        Logger logger = Mockito.mock(Logger.class);
        when(logger.isTraceEnabled()).thenReturn(false);
        when(logger.isDebugEnabled()).thenReturn(true);

        PerfLogger perfLogger = new PerfLogger(logger);
        long start = perfLogger.start();
        perfLogger.end(start, -1, "message", "argument");

        verify(logger, atLeastOnce()).isTraceEnabled();
        verify(logger, atLeastOnce()).isDebugEnabled();
        verify(logger, times(1)).debug(anyString(), any(Object[].class));
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void testEndSkipsIsDebugEnabled() {
        Logger logger = Mockito.mock(Logger.class);
        when(logger.isTraceEnabled()).thenReturn(false);
        when(logger.isDebugEnabled()).thenReturn(false);

        PerfLogger perfLogger = new PerfLogger(logger);
        long start = perfLogger.start();
        perfLogger.end(start, -1, "message", "argument");

        verify(logger, never()).isTraceEnabled();
        verify(logger, times(1)).isDebugEnabled();
        verifyNoMoreInteractions(logger);
    }

}
