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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.BasicStatusManager;
import ch.qos.logback.core.status.StatusManager;
import ch.qos.logback.core.status.StatusUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link LoggingInitializer}.
 */
public class LoggingInitializerTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private LoggerContext context;
    private LoggerContext sharedContext;
    private MockedStatic<LoggerFactory> loggerFactory;
    private String originalWorkDir;
    private String originalConfigFile;

    @Before
    public void saveLoggingConfiguration() {
        sharedContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        context = new LoggerContext();
        context.setMDCAdapter(new LogbackMDCAdapter());
        originalWorkDir = System.getProperty("oak.workDir");
        originalConfigFile = System.getProperty(ContextInitializer.CONFIG_FILE_PROPERTY);
        loggerFactory = Mockito.mockStatic(LoggerFactory.class, Mockito.CALLS_REAL_METHODS);
        loggerFactory.when(LoggerFactory::getILoggerFactory).thenReturn(context);
        System.clearProperty(ContextInitializer.CONFIG_FILE_PROPERTY);
        context.reset();
        context.start();
    }

    @After
    public void restoreLoggingConfiguration() {
        try {
            // reset also cancels scanners if a failed initialization left the context stopped.
            context.reset();
            context.stop();
        } finally {
            try {
                restoreProperty("oak.workDir", originalWorkDir);
                restoreProperty(ContextInitializer.CONFIG_FILE_PROPERTY, originalConfigFile);
            } finally {
                if (loggerFactory != null) {
                    loggerFactory.closeOnDemand();
                }
            }
        }
    }

    /** Reinitialization restores working file logging after shutdown. */
    @Test
    public void reinitializationStartsStoppedContext() throws Exception {
        File firstWorkDir = temporaryFolder.newFolder();
        new LoggingInitializer(firstWorkDir, "lifecycle").init();
        assertLoggingWorks(firstWorkDir, "before shutdown");
        LoggingInitializer.shutdownLogging();
        Assert.assertFalse(context.isStarted());

        File secondWorkDir = temporaryFolder.newFolder();
        new LoggingInitializer(secondWorkDir, "lifecycle").init();
        Assert.assertTrue(context.isStarted());
        assertLoggingWorks(secondWorkDir, "after restart");
    }

    /** Every shutdown closes appenders and cancels scanners. */
    @Test
    public void repeatedShutdownStopsAppendersAndScanners() throws Exception {
        for (int i = 0; i < 2; i++) {
            File workDir = temporaryFolder.newFolder();
            new LoggingInitializer(workDir, "lifecycle").init();
            Appender<ILoggingEvent> appender = context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("file");
            Assert.assertNotNull(appender);
            Assert.assertTrue(appender.isStarted());
            Assert.assertTrue(new File(workDir, "lifecycle.log").isFile());
            assertLoggingWorks(workDir, "lifecycle " + i);
            List<ScheduledFuture<?>> scanners = new ArrayList<>(context.getCopyOfScheduledFutures());
            Assert.assertFalse(scanners.isEmpty());

            LoggingInitializer.shutdownLogging();

            Assert.assertFalse("shutdown must close the file appender", appender.isStarted());
            for (ScheduledFuture<?> scanner : scanners) {
                Assert.assertTrue("shutdown must cancel configuration scanning", scanner.isCancelled());
            }
            Assert.assertFalse(context.isStarted());
        }
    }

    /** Skipping reset must still restart a stopped context. */
    @Test
    public void initializationWithoutResetStartsStoppedContext() throws Exception {
        context.stop();
        new LoggingInitializer(temporaryFolder.newFolder(), "lifecycle", false).init();
        Assert.assertTrue(context.isStarted());
        LoggingInitializer.shutdownLogging();
        Assert.assertTrue(context.getCopyOfScheduledFutures().isEmpty());
        Assert.assertFalse(context.isStarted());
    }

    /** Failed configuration must leave the context stopped. */
    @Test
    public void invalidConfigurationDoesNotRestartStoppedContext() throws Exception {
        context.stop();
        StatusManager originalStatusManager = context.getStatusManager();
        context.setStatusManager(new BasicStatusManager());
        try {
            File workDir = temporaryFolder.newFolder();
            new LoggingInitializer(workDir, "invalid").init();

            Assert.assertTrue(new File(workDir, "logback-invalid.xml").isFile());
            Assert.assertTrue("configuration must fail with an XML parsing error",
                    new StatusUtil(context).hasXMLParsingErrors(0));
            Assert.assertFalse("failed configuration must not restart a stopped context", context.isStarted());
            Assert.assertTrue(context.getCopyOfScheduledFutures().isEmpty());
            Assert.assertFalse(context.getLogger(Logger.ROOT_LOGGER_NAME).iteratorForAppenders().hasNext());
        } finally {
            context.setStatusManager(originalStatusManager);
        }
    }

    /** An external configuration keeps ownership of its logging context. */
    @Test
    public void customConfigurationSkipsInitializationAndShutdown() throws Exception {
        File workDir = temporaryFolder.newFolder();
        new LoggingInitializer(workDir, "lifecycle").init();
        Appender<ILoggingEvent> appender = context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("file");
        List<ScheduledFuture<?>> scanners = new ArrayList<>(context.getCopyOfScheduledFutures());
        Assert.assertFalse(scanners.isEmpty());
        String configuredWorkDir = System.getProperty("oak.workDir");
        System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, new File(workDir, "logback-lifecycle.xml").getAbsolutePath());

        File otherWorkDir = temporaryFolder.newFolder();
        new LoggingInitializer(otherWorkDir, "lifecycle").init();
        LoggingInitializer.shutdownLogging();

        Assert.assertFalse(new File(otherWorkDir, "logback-lifecycle.xml").exists());
        Assert.assertEquals(configuredWorkDir, System.getProperty("oak.workDir"));
        Assert.assertSame(appender, context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("file"));
        Assert.assertTrue(context.isStarted());
        Assert.assertTrue(appender.isStarted());
        for (ScheduledFuture<?> scanner : scanners) {
            Assert.assertFalse(scanner.isCancelled());
        }
    }

    /** The isolated lifecycle leaves shared logging untouched. */
    @Test
    public void initializationDoesNotChangeSharedLoggingContext() throws Exception {
        boolean sharedStarted = sharedContext.isStarted();
        List<ScheduledFuture<?>> sharedScanners = new ArrayList<>(sharedContext.getCopyOfScheduledFutures());
        List<Appender<ILoggingEvent>> sharedAppenders = new ArrayList<>();
        sharedContext.getLogger(Logger.ROOT_LOGGER_NAME).iteratorForAppenders().forEachRemaining(sharedAppenders::add);

        File workDir = temporaryFolder.newFolder();
        new LoggingInitializer(workDir, "lifecycle").init();
        assertLoggingWorks(workDir, "isolated context");
        Assert.assertNotSame(sharedContext, context);
        restoreLoggingConfiguration();

        Assert.assertSame(sharedContext, LoggerFactory.getILoggerFactory());
        Assert.assertEquals(sharedStarted, sharedContext.isStarted());
        Assert.assertEquals(sharedScanners, sharedContext.getCopyOfScheduledFutures());
        List<Appender<ILoggingEvent>> remainingAppenders = new ArrayList<>();
        sharedContext.getLogger(Logger.ROOT_LOGGER_NAME).iteratorForAppenders().forEachRemaining(remainingAppenders::add);
        Assert.assertEquals(sharedAppenders, remainingAppenders);
        Assert.assertEquals(originalWorkDir, System.getProperty("oak.workDir"));
        Assert.assertEquals(originalConfigFile, System.getProperty(ContextInitializer.CONFIG_FILE_PROPERTY));
    }

    private void assertLoggingWorks(File workDir, String message) throws Exception {
        Appender<ILoggingEvent> appender = context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("file");
        Assert.assertNotNull(appender);
        Assert.assertTrue(appender.isStarted());
        Assert.assertFalse(context.getCopyOfScheduledFutures().isEmpty());
        LoggerFactory.getLogger(LoggingInitializerTest.class).info(message);
        String output = Files.readString(new File(workDir, "lifecycle.log").toPath(), StandardCharsets.UTF_8);
        Assert.assertTrue("configured file appender must receive the message", output.contains(message));
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }
}
