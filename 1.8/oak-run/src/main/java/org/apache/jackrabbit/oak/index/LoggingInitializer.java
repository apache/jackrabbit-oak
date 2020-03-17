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

package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures the logging based on logback-indexing.xml. This file
 * would be copied to work directory and then logging would be
 * configured based on that
 *
 * The log file is configured for auto scan so any change made while
 * oak-run is in progress would be picked up
 */
public class LoggingInitializer {
    private static final String LOGBACK_INDEX_XML = "logback-indexing.xml";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File workDir;

    public LoggingInitializer(File workDir) {
        this.workDir = workDir;
    }

    public void init() throws IOException {
        //If custom config file defined then disable the default logic
        if (System.getProperty(ContextInitializer.CONFIG_FILE_PROPERTY) != null) {
            return;
        }

        File config = copyDefaultConfig();
        configureLogback(config);
        log.info("Logging configured from {}", config.getAbsolutePath());
        log.info("Any change in logging config would be picked up");
        log.info("Logs would be written to {}", new File(workDir, "indexing.log"));
    }

    public static void shutdownLogging(){
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.stop();
    }

    private void configureLogback(File config) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

        try {
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            System.setProperty("oak.workDir", FilenameUtils.normalizeNoEndSeparator(workDir.getAbsolutePath()));
            // Call context.reset() to clear any previous configuration, e.g. default
            // configuration. For multi-step configuration, omit calling context.reset().
            context.reset();
            configurator.doConfigure(config);
        } catch (JoranException je) {
            // StatusPrinter will handle this
        }
        StatusPrinter.printInCaseOfErrorsOrWarnings(context);
    }

    private File copyDefaultConfig() throws IOException {
        URL url = getClass().getResource("/" + LOGBACK_INDEX_XML);
        File dest = new File(workDir, LOGBACK_INDEX_XML);
        try (InputStream is = url.openStream()) {
            FileUtils.copyInputStreamToFile(is, dest);
        }
        return dest;
    }
}
