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
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instances of this class determine whether a session needs to be refreshed base on
 * the current {@link SessionOperation operation} to be performed and past
 * {@link #refreshed() refreshes} and {@link #saved() saves}.
 * <p>
 * Before an operation is performed a session calls {@link #needsRefresh(SessionOperation)},
 * to determine whether the session needs to be refreshed first. To maintain a session strategy's
 * state sessions call {@link #refreshed()} right after each refresh operation and
 * {@link #saved()} right after each save operation.
 * <p>
 * {@code RefreshStrategy} is a composite of zero or more {@code RefreshStrategy} instances,
 * each of which covers a certain strategy.
 * @see Default
 * @see Once
 * @see Timed
 * @see LogOnce
 * @see ThreadSynchronising
 */
public class RefreshStrategy {
    private static final Logger log = LoggerFactory.getLogger(RefreshStrategy.class);

    private final RefreshStrategy[] refreshStrategies;

    /**
     * Create a new instance consisting of the composite of the passed {@code RefreshStrategy}
     * instances.
     * @param refreshStrategies  individual refresh strategies
     */
    public RefreshStrategy(RefreshStrategy... refreshStrategies) {
        this.refreshStrategies = refreshStrategies;
    }

    /**
     * Determine whether the session needs to refresh before {@code sessionOperation} is performed.
     * <p>
     * This implementation return {@code false} if either {@code sessionsOperation} is an refresh
     * operation or a save operation. Otherwise it returns {@code true} if and only if any of the
     * individual refresh strategies passed to the constructor returns {@code true}.
     * @param sessionOperation  operation about to be performed
     * @return  {@code true} if and only if the session needs to refresh.
     */
    public boolean needsRefresh(SessionOperation<?> sessionOperation) {
        // Don't refresh if this operation is a refresh operation itself or
        // a save operation, which does an implicit refresh
        if (sessionOperation.isRefresh() || sessionOperation.isSave()) {
            return false;
        }

        boolean refresh = false;
        // Don't shortcut here since the individual strategies rely on side effects of this call
        for (RefreshStrategy r : refreshStrategies) {
            refresh |= r.needsRefresh(sessionOperation);
        }
        return refresh;
    }

    /**
     * Called whenever a session has been refreshed.
     * <p>
     * This implementation forwards to the {@code refresh} method of the individual refresh
     * strategies passed to the constructor.
     */
    public void refreshed() {
        for (RefreshStrategy r : refreshStrategies) {
            r.refreshed();
        }
    }

    /**
     * Called whenever a session has been saved.
     * <p>
     * This implementation forwards to the {@code save} method of the individual refresh
     * strategies passed to the constructor.
     */
    public void saved() {
        for (RefreshStrategy r : refreshStrategies) {
            r.saved();
        }
    }

    /**
     * Accept the passed visitor.
     * <p>
     * This implementation forwards to the {@code accept} method of the individual refresh
     * strategies passed to the constructor.
     */
    public void accept(Visitor visitor) {
        for (RefreshStrategy r: refreshStrategies) {
            r.accept(visitor);
        }
    }

    /**
     * Visitor for traversing the composite.
     */
    public static class Visitor {
        public void visit(Default strategy) {}
        public void visit(Once strategy) {}
        public void visit(Timed strategy) {}
        public void visit(LogOnce strategy) {}
        public void visit(ThreadSynchronising strategy) {}
    }

    /**
     * This refresh strategy does wither always or never refresh depending of the value of the
     * {@code refresh} argument passed to its constructor.
     * <p>
     */
    public static class Default extends RefreshStrategy {

        /** A refresh strategy that always refreshed */
        public static RefreshStrategy ALWAYS = new Default(true);

        /** A refresh strategy that never refreshed */
        public static RefreshStrategy NEVER = new Default(false);

        /** Value returned from {@code needsRefresh} */
        protected boolean refresh;

        /**
         * @param refresh  value returned from {@code needsRefresh}
         */
        public Default(boolean refresh) {
            this.refresh = refresh;
        }

        /**
         * @return {@link #refresh}
         */
        @Override
        public boolean needsRefresh(SessionOperation<?> sessionOperation) {
            return refresh;
        }

        @Override
        public void refreshed() {
        }

        @Override
        public void saved() {
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }
    }

    /**
     * This refresh strategy refreshed exactly once when enabled. Calling
     * {@link #reset()} enables the strategy.
     */
    public static class Once extends Default {

        /** Visitor for resetting this refresh strategy */
        public static final Visitor RESETTING_VISITOR = new Visitor() {
            @Override
            public void visit(Once strategy) {
                strategy.reset();
            }
        };

        /**
         * @param enabled  whether this refresh strategy is initially enabled
         */
        public Once(boolean enabled) {
            super(enabled);
        }

        /**
         * Enable this refresh strategy
         */
        public void reset() {
            refresh = true;
        }

        @Override
        public void refreshed() {
            refresh = false;
        }

        @Override
        public void saved() {
            refresh = false;
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }
    }

    /**
     * This refresh strategy refreshes after a given timeout of inactivity.
     */
    public static class Timed extends RefreshStrategy {
        private final long interval;
        private long lastAccessed = System.currentTimeMillis();

        /**
         * @param interval  Interval in seconds after which a session should refresh if there was no
         *                  activity.
         */
        public Timed(long interval) {
            this.interval = MILLISECONDS.convert(interval, SECONDS);
        }

        /**
         * Called whenever {@code needsRefresh} determines that the time out interval was exceeded.
         * This default implementation always returns {@code true}. Descendants may override this
         * method to provide more refined behaviour.
         * @param timeElapsed  the time that elapsed since the session was last accessed.
         * @return {@code true}
         */
        protected boolean timeOut(long timeElapsed) {
            return true;
        }

        @Override
        public boolean needsRefresh(SessionOperation<?> sessionOperation) {
            long now = System.currentTimeMillis();
            long timeElapsed = now - lastAccessed;
            lastAccessed = now;
            return timeElapsed > interval && timeOut(timeElapsed);
        }

        @Override
        public void refreshed() {
            lastAccessed = System.currentTimeMillis();
        }

        @Override
        public void saved() {
            lastAccessed = System.currentTimeMillis();
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }
    }

    /**
     * This refresh strategy never refreshed the session but logs a warning if a session has been
     * idle for more than a given time.
     *
     * TODO replace logging with JMX monitoring. See OAK-941
     */
    public static class LogOnce extends Timed {
        private final Exception initStackTrace = new Exception("The session was created here:");

        private boolean warnIfIdle = true;

        /**
         * @param interval  Interval in seconds after which a warning is logged if there was no
         *                  activity.
         */
        public LogOnce(long interval) {
            super(interval);
        }

        /**
         * Log once
         * @param timeElapsed  the time that elapsed since the session was last accessed.
         * @return  {@code false}
         */
        @Override
        protected boolean timeOut(long timeElapsed) {
            if (warnIfIdle) {
                log.warn("This session has been idle for " + MINUTES.convert(
                        timeElapsed, MILLISECONDS) + " minutes and might be out of date. " +
                        "Consider using a fresh session or explicitly refresh the session.",
                        initStackTrace);
                warnIfIdle = false;
            }
            return false;
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }
    }

    /**
     * This refresh strategy synchronises session states across accesses within the same thread.
     */
    public static class ThreadSynchronising extends RefreshStrategy {
        /**
         * ThreadLocal instance to keep track of the save operations performed in the thread so far
         * This is is then used to determine if the current session needs to be refreshed to see the
         * changes done by another session in current thread.
         * <p>
         * <b>Note</b> - This thread local is never cleared. However, we only store
         * java.lang.Integer and do not derive from ThreadLocal such that (class loader)
         * leaks typically associated with thread locals do not occur.
         */
        private final ThreadLocal<Long> threadSaveCount;

        private long sessionSaveCount;

        /**
         * @param threadSaveCount  thread local for tracking thread local state.
         */
        public ThreadSynchronising(ThreadLocal<Long> threadSaveCount) {
            this.threadSaveCount = threadSaveCount;
            sessionSaveCount = getThreadSaveCount();
        }

        @Override
        public boolean needsRefresh(SessionOperation<?> sessionOperation) {
            // If the threadLocal counter differs from our seen sessionSaveCount so far then
            // some other session would have done a commit. If that is the case a refresh would
            // be required
            return getThreadSaveCount() != sessionSaveCount;
        }

        @Override
        public void refreshed() {
            // Avoid further refreshing if refreshed already
            sessionSaveCount = getThreadSaveCount();
        }

        @Override
        public void saved() {
            // Force refreshing on access through other sessions on the same thread
            threadSaveCount.set(sessionSaveCount = (getThreadSaveCount() + 1));
        }

        private long getThreadSaveCount() {
            Long c = threadSaveCount.get();
            return c == null ? 0 : c;
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }
    }
}
