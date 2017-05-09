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
package org.apache.jackrabbit.oak.commons.junit;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import java.util.List;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;

import com.google.common.collect.Lists;

/**
 * The LogCustomizer allows to enable log level for a specific logger and/or
 * filter the received logs this logger on a dedicated log level
 * 
 * <pre>
 * public class ConflictResolutionTest {
 * 
 *     private final LogCustomizer customLogs = LogCustomizer
 *             .forLogger(
 *                     &quot;org.apache.jackrabbit.oak.plugins.commit.MergingNodeStateDiff&quot;)
 *             .enable(Level.DEBUG).create();
 * 
 *     &#064;Before
 *     public void setup() throws RepositoryException {
 *         customLogs.starting();
 *     }
 * 
 *     &#064;After
 *     public void after() {
 *         customLogs.finished();
 *     }
 * 
 *     &#064;Test
 *     public void test() {
 *         List&lt;String&gt; myLogs = customLogs.getLogs();
 *         assertTrue(myLogs.size() == 1);
 *     }
 * 
 * }
 * </pre>
 */

public class LogCustomizer {

    public static LogCustomizerBuilder forRootLogger() {
        return forLogger(ROOT_LOGGER_NAME);
    }

    public static LogCustomizerBuilder forLogger(String name) {
        return new LogCustomizerBuilder(name);
    }

    public static class LogCustomizerBuilder {

        private final String name;
        private Level enableLevel;
        private Level filterLevel;
        private String matchExactMessage;
        private String matchContainsMessage;
        private String matchRegexMessage;

        private LogCustomizerBuilder(String name) {
            this.name = name;
        }

        public LogCustomizerBuilder enable(Level level) {
            this.enableLevel = level;
            return this;
        }

        public LogCustomizerBuilder filter(Level level) {
            this.filterLevel = level;
            return this;
        }

        public LogCustomizerBuilder exactlyMatches(String message) {
            this.matchExactMessage = message;
            return this;
        }

        public LogCustomizerBuilder contains(String message) {
            this.matchContainsMessage = message;
            return this;
        }

        public LogCustomizerBuilder matchesRegex(String message) {
            this.matchRegexMessage = message;
            return this;
        }

        public LogCustomizer create() {
            return new LogCustomizer(name, enableLevel, filterLevel, matchExactMessage, matchContainsMessage, matchRegexMessage);
        }
    }

    private final Logger logger;
    private final List<String> logs = Lists.newArrayList();

    private final Level enableLevel;
    private final Level originalLevel;

    private final Appender<ILoggingEvent> customLogger;

    private LogCustomizer(String name, Level enableLevel,
                          final Level filterLevel,
                          final String matchExactMessage, final String matchContainsMessage, final String matchRegexMessage) {
        this.logger = getLogger(name);
        if (enableLevel != null) {
            this.enableLevel = enableLevel;
            this.originalLevel = logger.getLevel();
        } else {
            this.enableLevel = null;
            this.originalLevel = null;
        }

        customLogger = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent e) {
                boolean logLevelOk = false;
                if (filterLevel == null) {
                    logLevelOk = true;
                } else if (e.getLevel().isGreaterOrEqual(filterLevel)) {
                    logLevelOk = true;
                }

                if(logLevelOk) {
                    boolean messageMatchOk = true;
                    String message = e.getFormattedMessage();

                    if (messageMatchOk && matchExactMessage != null && !matchExactMessage.equals(message)) {
                        messageMatchOk = false;
                    }

                    if (messageMatchOk && matchContainsMessage != null && !message.contains(matchContainsMessage)) {
                        messageMatchOk = false;
                    }

                    if (messageMatchOk && matchRegexMessage != null && !message.matches(matchRegexMessage)) {
                        messageMatchOk = false;
                    }

                    if (messageMatchOk) {
                        logs.add(e.getFormattedMessage());
                    }
                }
            }
        };
    }

    private static Logger getLogger(String name) {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(name);
    }

    public List<String> getLogs() {
        return logs;
    }

    public void starting() {
        customLogger.start();
        if (enableLevel != null) {
            logger.setLevel(enableLevel);
        }
        logger.addAppender(customLogger);
    }

    public void finished() {
        if (originalLevel != null) {
            logger.setLevel(originalLevel);
        }
        logger.detachAppender(customLogger);
        customLogger.stop();
        logs.clear();
    }

}
