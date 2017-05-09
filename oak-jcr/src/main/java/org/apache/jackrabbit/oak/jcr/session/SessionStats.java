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
package org.apache.jackrabbit.oak.jcr.session;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.jmx.SessionMBean;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticManager;

public class SessionStats implements SessionMBean {

    /**
     * The threshold of active sessions from where on it should record the stack trace for new sessions. The reason why
     * this is not enabled by default is because recording stack traces is rather expensive and can significantly
     * slow down the code if sessions are created and thrown away in a loop.
     *
     * Once this threshold is exceeded, we assume that there is a session leak which should be fixed and start recording
     * the stack traces to make it easier to find the cause of it.
     *
     * Configurable by the "oak.sessionStats.initStackTraceThreshold" system property. Set to "0" to record stack trace
     * information on each session creation.
     *
     */
    static final int INIT_STACK_TRACE_THRESHOLD = Integer.getInteger("oak.sessionStats.initStackTraceThreshold", 1000);

    private final Exception initStackTrace;

    private final AtomicReference<RepositoryException> lastFailedSave =
            new AtomicReference<RepositoryException>();

    private final Counters counters;
    private final String sessionId;
    private final AuthInfo authInfo;
    private final Clock clock;
    private final RefreshStrategy refreshStrategy;

    private volatile SessionDelegate sessionDelegate;

    private Map<String, Object> attributes = Collections.emptyMap();

    public SessionStats(String sessionId, AuthInfo authInfo, Clock clock,
            RefreshStrategy refreshStrategy, SessionDelegate sessionDelegate, StatisticManager statisticManager) {
        this.counters = new Counters(clock);
        this.sessionId = sessionId;
        this.authInfo = authInfo;
        this.clock = clock;
        this.refreshStrategy = refreshStrategy;
        this.sessionDelegate = sessionDelegate;

        long activeSessionCount = statisticManager.getStatsCounter(Type.SESSION_COUNT).getCount();
        initStackTrace = (activeSessionCount > INIT_STACK_TRACE_THRESHOLD) ?
                new Exception("The session was opened here:") : null;
    }

    public void close() {
        sessionDelegate = null;
    }

    public static class Counters {
        private final Clock clock;
        private final long loginTime;
        public long accessTime;
        public long readTime = 0;
        public long writeTime = 0;
        public long refreshTime = 0;
        public long saveTime = 0;
        public long readCount = 0;
        public long writeCount = 0;
        public long refreshCount = 0;
        public long saveCount = 0;

        public Counters(Clock clock) {
            long time = clock.getTime();
            this.clock = clock;
            this.loginTime = time;
            this.accessTime = time;
        }

        public Date getLoginTime() {
            return new Date(loginTime);
        }

        public Date getReadTime() {
            return getTime(readTime);
        }

        public long getReadCount() {
            return readCount;
        }

        public Date getWriteTime() {
            return getTime(writeTime);
        }

        public long getWriteCount() {
            return writeCount;
        }

        public Date getRefreshTime() {
            return getTime(refreshTime);
        }

        public long getRefreshCount() {
            return refreshCount;
        }

        public Date getSaveTime() {
            return getTime(saveTime);
        }

        public long getSaveCount() {
            return saveCount;
        }

        public long getSecondsSinceLogin() {
            return SECONDS.convert(clock.getTime() - loginTime, MILLISECONDS);
        }

        private static Date getTime(long timestamp) {
            if (timestamp != 0) {
                return new Date(timestamp);
            } else {
                return null;
            }
        }
    }

    public Counters getCounters() {
        return counters;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public void failedSave(RepositoryException repositoryException) {
        lastFailedSave.set(repositoryException);
    }

    @Override
    public String toString() {
        return getAuthInfo().getUserID() + '@' + sessionId + '@' + getLoginTimeStamp();
    }

    //------------------------------------------------------------< SessionMBean >---

    @Override
    public String getInitStackTrace() {
        return format(initStackTrace);
    }

    @Override
    public AuthInfo getAuthInfo() {
        return authInfo;
    }

    @Override
    public String getLoginTimeStamp() {
        return formatDate(counters.getLoginTime());
    }

    @Override
    public String getLastReadAccess() {
        return formatDate(counters.getReadTime());
    }

    @Override
    public long getReadCount() {
        return counters.getReadCount();
    }

    @Override
    public double getReadRate() {
        return calculateRate(getReadCount());
    }

    @Override
    public String getLastWriteAccess() {
        return formatDate(counters.getWriteTime());
    }

    @Override
    public long getWriteCount() {
        return counters.getWriteCount();
    }

    @Override
    public double getWriteRate() {
        return calculateRate(getWriteCount());
    }

    @Override
    public String getLastRefresh() {
        return formatDate(counters.getRefreshTime());
    }

    @Override
    public String getRefreshStrategy() {
        return refreshStrategy.toString();
    }

    @Override
    public boolean getRefreshPending() {
        return refreshStrategy.needsRefresh(
                SECONDS.convert(clock.getTime() - counters.accessTime, MILLISECONDS));
    }

    @Override
    public long getRefreshCount() {
        return counters.getRefreshCount();
    }

    @Override
    public double getRefreshRate() {
        return calculateRate(getRefreshCount());
    }

    @Override
    public String getLastSave() {
        return formatDate(counters.getSaveTime());
    }

    @Override
    public long getSaveCount() {
        return counters.getSaveCount();
    }

    @Override
    public double getSaveRate() {
        return calculateRate(getSaveCount());
    }

    @Override
    public String[] getSessionAttributes() {
        String[] atts = new String[attributes.size()];
        int k = 0;
        for (Entry<String, Object> attribute : attributes.entrySet()) {
            atts[k++] = attribute.getKey() + '=' + attribute.getValue();
        }
        return atts;
    }

    @Override
    public String getLastFailedSave() {
        return format(lastFailedSave.get());
    }

    @Override
    public void refresh() {
        final SessionDelegate sd = sessionDelegate;
        if (sd != null) {
            sd.safePerform(new SessionOperation<Void>("MBean initiated refresh", true) {
                @Override
                public Void perform() {
                    sd.refresh(true);
                    return null;
                }

                @Override
                public void checkPreconditions() throws RepositoryException {
                    sd.checkAlive();
                }
            });
            sd.refresh(true);
        }
    }

    //------------------------------------------------------------< internal >---

    private static String formatDate(Date date) {
        return date == null
            ? ""
            : DateFormat.getDateTimeInstance().format(date);
    }

    private static String format(Exception exception) {
        if (exception == null) {
            return "";
        } else {
            StringWriter writer = new StringWriter();
            exception.printStackTrace(new PrintWriter(writer));
            return writer.toString();
        }
    }

    private double calculateRate(long count) {
        double dt = counters.getSecondsSinceLogin();
        if (dt > 0) {
            return count / dt;
        } else {
            return Double.NaN;
        }
    }

}
