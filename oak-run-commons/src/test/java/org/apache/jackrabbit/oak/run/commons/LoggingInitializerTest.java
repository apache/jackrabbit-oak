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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.BasicStatusManager;
import ch.qos.logback.core.model.Model;
import ch.qos.logback.core.model.ModelUtil;
import ch.qos.logback.core.status.StatusManager;
import ch.qos.logback.core.status.StatusUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link LoggingInitializer}.
 */
public class LoggingInitializerTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private LoggerContext context;
    private Model originalConfiguration;
    private boolean originallyStarted;
    private String originalWorkDir;
    private String originalConfigFile;

    @Before
    public void saveLoggingConfiguration() {
        context = (LoggerContext) LoggerFactory.getILoggerFactory();
        originallyStarted = context.isStarted();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        originalConfiguration = configurator.recallSafeConfiguration();
        originalWorkDir = System.getProperty("oak.workDir");
        originalConfigFile = System.getProperty(ContextInitializer.CONFIG_FILE_PROPERTY);
        System.clearProperty(ContextInitializer.CONFIG_FILE_PROPERTY);
        context.reset();
        context.start();
    }

    @After
    public void restoreLoggingConfiguration() throws Exception {
        // reset also cancels scanners if a failed initialization left the context stopped.
        context.reset();
        restoreProperty("oak.workDir", originalWorkDir);
        restoreProperty(ContextInitializer.CONFIG_FILE_PROPERTY, originalConfigFile);
        if (originalConfiguration != null) {
            ModelUtil.resetForReuse(originalConfiguration);
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            configurator.processModel(originalConfiguration);
            configurator.registerSafeConfiguration(originalConfiguration);
        } else {
            new ContextInitializer(context).autoConfig();
        }
        context.start();
        if (!originallyStarted) {
            context.stop();
        }
    }

    @Test
    public void reinitializationStartsStoppedContext() throws Exception {
        new LoggingInitializer(temporaryFolder.newFolder(), "lifecycle").init();
        LoggingInitializer.shutdownLogging();
        Assert.assertFalse(context.isStarted());

        new LoggingInitializer(temporaryFolder.newFolder(), "lifecycle").init();
        Assert.assertTrue(context.isStarted());
    }

    @Test
    public void repeatedShutdownStopsAppendersAndScanners() throws Exception {
        for (int i = 0; i < 2; i++) {
            File workDir = temporaryFolder.newFolder();
            new LoggingInitializer(workDir, "lifecycle").init();
            Appender<ILoggingEvent> appender = context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("file");
            Assert.assertNotNull(appender);
            Assert.assertTrue(appender.isStarted());
            Assert.assertTrue(new File(workDir, "lifecycle.log").isFile());
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

    @Test
    public void initializationWithoutResetStartsStoppedContext() throws Exception {
        context.stop();
        new LoggingInitializer(temporaryFolder.newFolder(), "lifecycle", false).init();
        Assert.assertTrue(context.isStarted());
        LoggingInitializer.shutdownLogging();
        Assert.assertTrue(context.getCopyOfScheduledFutures().isEmpty());
        Assert.assertFalse(context.isStarted());
    }

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

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }
}
