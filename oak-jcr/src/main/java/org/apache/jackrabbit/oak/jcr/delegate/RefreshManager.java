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

package org.apache.jackrabbit.oak.jcr.delegate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import org.apache.jackrabbit.oak.jcr.operation.SessionOperation;

/**
 * This class contains the auto refresh logic for sessions, which is done to enhance backwards
 * compatibility with Jackrabbit 2.
 * <p>
 * A sessions is automatically refreshed when
 * <ul>
 *     <li>it has not been accessed for the number of seconds specified by the
 *         {@code refreshInterval} parameter,</li>
 *     <li>an observation event has been delivered to a listener registered from within this
 *         session,</li>
 *     <li>an updated occurred through a different session from <em>within the same
 *         thread.</em></li>
 * </ul>
 */
public class RefreshManager {
    private final Exception initStackTrace = new Exception("The session was created here:");
    private final long refreshInterval;

    /**
     * ThreadLocal instance to keep track of the save operations performed in the thread so far
     * This is is then used to determine if the current session needs to be refreshed to see the
     * changes done by another session in current thread.
     * <p>
     * <b>Note</b> - This thread local is never cleared. However, we only store
     * java.lang.Integer and do not derive from ThreadLocal such that (class loader)
     * leaks typically associated with thread locals do not occur.
     */
    private final ThreadLocal<Integer> threadSaveCount;

    private long lastAccessed = System.currentTimeMillis();
    private boolean warnIfIdle = true;
    private boolean refreshAtNextAccess;
    private int sessionSaveCount;

    public RefreshManager(long refreshInterval, ThreadLocal<Integer> threadSaveCount) {
        this.refreshInterval = refreshInterval;
        this.threadSaveCount = threadSaveCount;

        sessionSaveCount = getOr0(threadSaveCount);
    }

    /**
     * Called before the passed {@code sessionOperation} is performed. This method
     * refreshes the session according to the rules given in the class comment.
     *
     * @param delegate  session on which the {@code sessionOperation} is executed
     * @param sessionOperation  the operation to be executed
     * @return  {@code true} if a refreshed, {@code false} otherwise.
     */
    boolean refreshIfNecessary(SessionDelegate delegate, SessionOperation<?> sessionOperation) {
        long now = System.currentTimeMillis();
        long timeElapsed = now - lastAccessed;
        lastAccessed = now;

        // Don't refresh if this operation is a refresh operation itself or
        // a save operation, which does an implicit refresh
        if (!sessionOperation.isRefresh() && !sessionOperation.isSave()) {
            if (warnIfIdle && !refreshAtNextAccess
                    && timeElapsed > MILLISECONDS.convert(1, MINUTES)) {
                // Warn once if this session has been idle too long
                SessionDelegate.log.warn("This session has been idle for " + MINUTES.convert(timeElapsed, MILLISECONDS) +
                        " minutes and might be out of date. Consider using a fresh session or explicitly" +
                        " refresh the session.", initStackTrace);
                warnIfIdle = false;
            }
            if (refreshAtNextAccess || hasInThreadCommit() || timeElapsed >= refreshInterval) {
                // Refresh if forced or if the session has been idle too long
                refreshAtNextAccess = false;
                sessionSaveCount = getOr0(threadSaveCount);
                delegate.refresh(true);
                return true;
            }
        }

        if (sessionOperation.isSave()) {
            threadSaveCount.set(sessionSaveCount = (getOr0(threadSaveCount) + 1));
        }

        return false;
    }

    void refreshAtNextAccess() {
        refreshAtNextAccess = true;
    }

    private boolean hasInThreadCommit() {
        // If the threadLocal counter differs from our seen sessionSaveCount so far then
        // some other session would have done a commit. If that is the case a refresh would
        // be required
        return getOr0(threadSaveCount) != sessionSaveCount;
    }

    private static int getOr0(ThreadLocal<Integer> threadLocal) {
        Integer c = threadLocal.get();
        return c == null ? 0 : c;
    }
}
