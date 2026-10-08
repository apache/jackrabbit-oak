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
package org.apache.jackrabbit.oak.run.commons;

import java.io.File;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.Appender;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests logging resource cleanup in {@link LoggingInitializer}. */
public class LoggingInitializerTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /** Repeated initialization and shutdown must close appenders and cancel scanners. */
    @Test
    public void repeatedShutdownStopsAppendersAndScanners() throws Exception {
        String workDir = System.getProperty("oak.workDir");
        String configFile = System.getProperty(ContextInitializer.CONFIG_FILE_PROPERTY);
        LoggerContext context = new LoggerContext();
        context.setMDCAdapter(new LogbackMDCAdapter());
        context.start();
        try (MockedStatic<LoggerFactory> factory = Mockito.mockStatic(LoggerFactory.class, Mockito.CALLS_REAL_METHODS)) {
            factory.when(LoggerFactory::getILoggerFactory).thenReturn(context);
            System.clearProperty(ContextInitializer.CONFIG_FILE_PROPERTY);
            for (int i = 0; i < 2; i++) {
                new LoggingInitializer(temporaryFolder.newFolder(), "lifecycle").init();
                Appender<ILoggingEvent> appender = context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("file");
                Assert.assertNotNull(appender);
                Assert.assertTrue(appender.isStarted());
                List<ScheduledFuture<?>> scanners = context.getCopyOfScheduledFutures();
                Assert.assertFalse(scanners.isEmpty());

                LoggingInitializer.shutdownLogging();

                Assert.assertFalse("shutdown must close the file appender", appender.isStarted());
                for (ScheduledFuture<?> scanner : scanners) {
                    Assert.assertTrue("shutdown must cancel configuration scanning", scanner.isCancelled());
                }
            }
        } finally {
            try {
                context.reset();
                context.stop();
            } finally {
                restoreProperty("oak.workDir", workDir);
                restoreProperty(ContextInitializer.CONFIG_FILE_PROPERTY, configFile);
            }
        }
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) { System.clearProperty(name); }
        else { System.setProperty(name, value); }
    }
}
