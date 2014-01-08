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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.jmx.SessionMBean;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;

public class SessionStats implements SessionMBean {
    private static final long EPS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);

    private final Exception initStackTrace = new Exception("The session was opened here:");
    private final Date loginTimeStamp = new Date();
    private final AtomicReference<Date> lastReadAccess = new AtomicReference<Date>();
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicReference<Date> lastWriteAccess = new AtomicReference<Date>();
    private final AtomicLong writeCount = new AtomicLong();
    private final AtomicReference<Date> lastRefresh = new AtomicReference<Date>();
    private final AtomicLong refreshCount = new AtomicLong();
    private final AtomicReference<Date> lastSave = new AtomicReference<Date>();
    private final AtomicLong saveCount = new AtomicLong();
    private final AtomicReference<RepositoryException> lastFailedSave =
            new AtomicReference<RepositoryException>();

    private final String sessionId;
    private final AuthInfo authInfo;

    private Map<String, Object> attributes = Collections.emptyMap();

    public SessionStats(SessionDelegate sessionDelegate) {
        this.sessionId = sessionDelegate.toString();
        this.authInfo = sessionDelegate.getAuthInfo();
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public void read() {
        // There is a small race here and in the following methods, which might lead to
        // a small window of inconsistency between the date and the count. Since the
        // effect is small and the provided information is for information purposes
        // this is preferable to synchronizing the method.
        lastReadAccess.set(new Date());
        readCount.incrementAndGet();
    }

    public void write() {
        lastWriteAccess.set(new Date());
        writeCount.incrementAndGet();
    }

    public void refresh() {
        lastRefresh.set(new Date());
        refreshCount.incrementAndGet();
    }

    public void save() {
        lastSave.set(new Date());
        saveCount.incrementAndGet();
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
        return formatDate(loginTimeStamp);
    }

    @Override
    public String getLastReadAccess() {
        return formatDate(lastReadAccess.get());
    }

    @Override
    public long getReadCount() {
        return readCount.get();
    }

    @Override
    public double getReadRate() {
        return calculateRate(getReadCount());
    }

    @Override
    public String getLastWriteAccess() {
        return formatDate(lastWriteAccess.get());
    }

    @Override
    public long getWriteCount() {
        return writeCount.get();
    }

    @Override
    public double getWriteRate() {
        return calculateRate(getWriteCount());
    }

    @Override
    public String getLastRefresh() {
        return formatDate(lastRefresh.get());
    }

    @Override
    public long getRefreshCount() {
        return refreshCount.get();
    }

    @Override
    public double getRefreshRate() {
        return calculateRate(getRefreshCount());
    }

    @Override
    public String getLastSave() {
        return formatDate(lastSave.get());
    }

    @Override
    public long getSaveCount() {
        return saveCount.get();
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
            atts[k] = attribute.getKey() + '=' + attribute.getValue();
        }
        return atts;
    }

    @Override
    public String getLastFailedSave() {
        return format(lastFailedSave.get());
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
        double dt = System.currentTimeMillis() - loginTimeStamp.getTime();
        if (dt > EPS) {
            return count / dt;
        } else {
            return Double.NaN;
        }
    }

}
