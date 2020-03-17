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

import java.io.PrintWriter;
import java.io.StringWriter;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.CyclicBufferAppender;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

/**
 * The LogDumper Rule collects logs which are generated due to execution of test  and dumps them
 * locally upon test failure. This simplifies determining failure
 * cause by providing all required data locally. This would be specially useful when running test
 * in CI server where server logs gets cluttered with all other test executions
 * <p/>
 * <pre>
 *     public class LoginTestIT {
 *
 *     &#064;Rule
 *     public TestRule logDumper = new LogDumper();
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
public class LogDumper extends TestWatcher {
    /**
     * Number of log entries to keep in memory
     */
    private static final int LOG_BUFFER_SIZE = 1000;

    /**
     * Message pattern used to render logs
     */
    private static final String DEFAULT_PATTERN = "%d{dd.MM.yyyy HH:mm:ss.SSS} *%level* [%thread] %logger %msg%n";

    private CyclicBufferAppender<ILoggingEvent> appender;

    private final int logBufferSize;

    /**
     * Creates a new LogDumper with default log buffer size (1000)
     */
    public LogDumper() {
        this(LOG_BUFFER_SIZE);
    }
    
    /**
     * Creates a new LogDumper with the given log buffer size
     * @param logBufferSize
     */
    public LogDumper(int logBufferSize) {
        this.logBufferSize = logBufferSize;
    }
    
    @Override
    protected void finished(Description description) {
        deregisterAppender();
    }

    @Override
    protected void starting(Description description) {
        registerAppender();
    }

    @Override
    protected void failed(Throwable e, Description description) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);

        try {
            String msg = e.getMessage();
            if (msg != null) {
                pw.println(msg);
            }

            pw.printf("=============== Logs for [%s#%s]===================%n",
                    description.getClassName(), description.getMethodName());
            pw.print(getLogs());
            pw.println("========================================================");


        } catch (Throwable t) {
            System.err.println("Error occurred while fetching test logs");
            t.printStackTrace(System.err);
        }

        System.err.print(sw.toString());
    }

    private String getLogs() {
        if (appender == null) {
            return "<Logs cannot be determined>";
        }
        PatternLayout layout = createLayout();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < appender.getLength(); i++) {
            sb.append(layout.doLayout(appender.get(i)));
        }
        return sb.toString();
    }

    private void registerAppender() {
        appender = new CyclicBufferAppender<ILoggingEvent>();
        appender.setMaxSize(logBufferSize);
        appender.setContext(getContext());
        appender.setName("TestLogCollector");
        appender.start();
        rootLogger().addAppender(appender);
    }

    private void deregisterAppender() {
        if (appender != null) {
            rootLogger().detachAppender(appender);
            appender.stop();
            appender = null;
        }
    }

    private static PatternLayout createLayout() {
        PatternLayout pl = new PatternLayout();
        pl.setPattern(DEFAULT_PATTERN);
        pl.setOutputPatternAsHeader(false);
        pl.setContext(getContext());
        pl.start();
        return pl;
    }

    private static LoggerContext getContext() {
        return (LoggerContext) LoggerFactory.getILoggerFactory();
    }

    private static ch.qos.logback.classic.Logger rootLogger() {
        return getContext().getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    }
}
