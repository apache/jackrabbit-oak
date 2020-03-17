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

import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.filter.Filter;

/**
 * The LogLevelModifier Rule can be used to fine-tune log levels during a particular
 * test. This could be used together with LogDumper to have enough details
 * in case of test failure without setting the global log level to DEBUG for example.
 * <p/>
 * <pre>
 *     public class LoginTestIT {
 *
 *     &#064;Rule
 *     public TestRule logDumper = new LogLevelModifier()
 *                                         .addAppenderFilter("console", "warn")
 *                                         .setLoggerLevel("org.apache.jackrabbit.oak", "debug");
 *
 *     &#064;Test
 *     public void remoteLogin() {
 *          //test stuff
 *          assertEquals(&quot;testA&quot;, name.getMethodName());
 *     }
 *
 *     }
 * </pre>
 */
public class LogLevelModifier extends TestWatcher {

    class AppenderFilter {
        
        private final Appender<ILoggingEvent> appender;
        private final String level;
        private ThresholdFilter thFilter;

        AppenderFilter(String appenderName, String level) {
            final Appender<ILoggingEvent> appender = rootLogger().getAppender(appenderName);
            if (appender==null) {
                fail("no appender found with name "+appenderName);
            }
            this.appender = appender;
            Level l = Level.toLevel(level, null);
            if (l==null) {
                fail("unknown level: "+level);
            }
            this.level = l.levelStr;
        }

        public void starting() {
            thFilter = new ThresholdFilter();
            thFilter.setLevel(level);
            thFilter.start();
            appender.addFilter(thFilter);
        }

        public void finished() {
            if (thFilter==null) {
                // then we did not add it
                return;
            }
            List<Filter<ILoggingEvent>> filterList = appender.getCopyOfAttachedFiltersList();
            appender.clearAllFilters();
            for (Iterator<Filter<ILoggingEvent>> it = filterList.iterator(); it.hasNext();) {
                Filter<ILoggingEvent> filter = it.next();
                if (filter!=thFilter) {
                    appender.addFilter(filter);
                }
            }
        }
    }
    
    class LoggerLevel {
        
        private final Logger logger;
        private final Level previousLevel;
        private Level level;

        LoggerLevel(String loggerName, String level) {
            final LoggerContext c = getContext();
            Logger existing = c.exists(loggerName);
            if (existing!=null) {
                logger = existing;
                previousLevel = existing.getLevel();
            } else {
                logger = c.getLogger(loggerName);
                previousLevel = null;
            }
            Level l = Level.toLevel(level, null);
            if (l==null) {
                fail("unknown level: "+level);
            }
            this.level = l;
        }

        public void starting() {
            logger.setLevel(level);
        }

        public void finished() {
            logger.setLevel(previousLevel);
        }
    }
    
    private final List<AppenderFilter> appenderFilters = new LinkedList<AppenderFilter>();
    @SuppressWarnings("rawtypes")
    private final List<Appender> newAppenders = new LinkedList<Appender>();
    private final List<LoggerLevel> loggerLevels = new LinkedList<LoggerLevel>();
    
    public LogLevelModifier newConsoleAppender(String name) {
        ConsoleAppender<ILoggingEvent> c = new ConsoleAppender<ILoggingEvent>();
        c.setName(name);
        c.setContext(getContext());
        c.start();
        rootLogger().addAppender(c);
        newAppenders.add(c);
        return this;
    }
    
    /** 
     * Adds a ThresholdFilter with the given level to an existing appender during the test.
     * <p>
     * Note that unless you filter existing appenders, changing the log level via setLoggerLevel
     * will, as a side-effect, also apply and influence existing appenders. So the
     * idea is to do eg addAppenderFilter("console", "warn") to make sure nothing
     * gets logged on console, when changing a log level.
     */
    public LogLevelModifier addAppenderFilter(String appenderName, String level) {
        appenderFilters.add(new AppenderFilter(appenderName, level));
        return this;
    }
    
    /** Change the log level of a particular logger during the test **/
    public LogLevelModifier setLoggerLevel(String loggerName, String level) {
        loggerLevels.add(new LoggerLevel(loggerName, level));
        return this;
    }
    
    @Override
    protected void starting(Description description) {
        for (Iterator<AppenderFilter> it = appenderFilters.iterator(); it.hasNext();) {
            AppenderFilter appenderFilter = (AppenderFilter) it.next();
            appenderFilter.starting();
        }
        for (Iterator<LoggerLevel> it = loggerLevels.iterator(); it.hasNext();) {
            LoggerLevel loggerLevel = (LoggerLevel) it.next();
            loggerLevel.starting();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void finished(Description description) {
        for (Iterator<AppenderFilter> it = appenderFilters.iterator(); it.hasNext();) {
            AppenderFilter appenderFilter = (AppenderFilter) it.next();
            appenderFilter.finished();
        }
        for (Iterator<LoggerLevel> it = loggerLevels.iterator(); it.hasNext();) {
            LoggerLevel loggerLevel = (LoggerLevel) it.next();
            loggerLevel.finished();
        }
        for (Iterator<Appender> it = newAppenders.iterator(); it.hasNext();) {
            Appender appender = it.next();
            rootLogger().detachAppender(appender);
        }
    }

    @Override
    protected void failed(Throwable e, Description description) {
        // nothing to do here
    }

    private static LoggerContext getContext() {
        return (LoggerContext) LoggerFactory.getILoggerFactory();
    }

    private static ch.qos.logback.classic.Logger rootLogger() {
        return getContext().getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    }
}
