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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementations of this interface determine whether a session needs
 * to be refreshed before the next session operation is performed. This is
 * done by the session calling {@link #needsRefresh(long)} to determine
 * whether a refresh is needed.
 *
 * @see Composite
 * @see Timed
 * @see LogOnce
 */
public interface RefreshStrategy {

    /**
     * Determine whether the given session needs to refresh before the next
     * session operation is performed.
     * <p>
     * This implementation returns {@code true} if and only if any of the
     * individual refresh strategies passed to the constructor returns
     * {@code true}.
     *
     * @param secondsSinceLastAccess seconds since last access
     * @return  {@code true} if and only if the session needs to refresh.
     */
    boolean needsRefresh(long secondsSinceLastAccess);

    void refreshed();

    /**
     * Composite of zero or more {@code RefreshStrategy} instances,
     * each of which covers a certain strategy.
     */
    class Composite implements RefreshStrategy {

        private final RefreshStrategy[] refreshStrategies;

        public static RefreshStrategy create(RefreshStrategy... refreshStrategies) {
            ArrayList<RefreshStrategy> strategies = newArrayList();
            for (RefreshStrategy strategy : refreshStrategies) {
                if (strategy instanceof Composite) {
                    strategies.addAll(asList(((Composite) strategy).refreshStrategies));
                } else {
                    strategies.add(strategy);
                }
            }
            return new Composite(strategies.toArray(new RefreshStrategy[strategies.size()]));
        }

        /**
         * Create a new instance consisting of the composite of the
         * passed {@code RefreshStrategy} instances.
         * @param refreshStrategies  individual refresh strategies
         */
        private Composite(RefreshStrategy... refreshStrategies) {
            this.refreshStrategies = refreshStrategies;
        }

        /**
         * Determine whether the given session needs to refresh before the next
         * session operation is performed.
         * <p>
         * This implementation returns {@code true} if and only if any of the
         * individual refresh strategies passed to the constructor returns
         * {@code true}.
         *
         * @param secondsSinceLastAccess seconds since last access
         * @return  {@code true} if and only if the session needs to refresh.
         */
        @Override
        public boolean needsRefresh(long secondsSinceLastAccess) {
            for (RefreshStrategy r : refreshStrategies) {
                if (r.needsRefresh(secondsSinceLastAccess)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void refreshed() {
            for (RefreshStrategy refreshStrategy : refreshStrategies) {
                refreshStrategy.refreshed();
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            String sep = "";
            for (RefreshStrategy strategy : refreshStrategies) {
                sb.append(sep).append(strategy.toString());
                sep = ", ";
            }
            return sb.toString();
        }
    }

    /**
     * This refresh strategy refreshes after a given timeout of inactivity.
     */
    class Timed implements RefreshStrategy {

        protected final long interval;

        /**
         * @param interval  Interval in seconds after which a session should refresh if there was no
         *                  activity.
         */
        public Timed(long interval) {
            this.interval = interval;
        }

        @Override
        public boolean needsRefresh(long secondsSinceLastAccess) {
            return secondsSinceLastAccess > interval;
        }

        @Override
        public void refreshed() {
            // empty
        }

        @Override
        public String toString() {
            return "Refresh every " + interval + " seconds";
        }
    }

    /**
     * This refresh strategy never refreshed the session but logs
     * a warning if a session has been idle for more than a given time.
     *
     * TODO replace logging with JMX monitoring. See OAK-941
     */
    class LogOnce extends Timed {

        private static final Logger log =
                LoggerFactory.getLogger(RefreshStrategy.class);

        private final Exception initStackTrace =
                new Exception("The session was created here:");

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
         * @param secondsSinceLastAccess seconds since last access
         * @return {@code false}
         */
        @Override
        public boolean needsRefresh(long secondsSinceLastAccess) {
            if (super.needsRefresh(secondsSinceLastAccess) && warnIfIdle) {
                log.warn("This session has been idle for "
                        + MINUTES.convert(secondsSinceLastAccess, SECONDS)
                        + " minutes and might be out of date. " +
                        "Consider using a fresh session or explicitly refresh the session.",
                        initStackTrace);
            }
            return false;
        }

        @Override
        public void refreshed() {
            warnIfIdle = false;
        }

        @Override
        public String toString() {
            return "Never refresh but log warning after more than " + interval + " seconds of inactivity";
        }
    }

    /**
     * This strategy conditionally invokes the delegated strategy based on the passed predicate
     */
    class ConditionalRefreshStrategy implements RefreshStrategy {
        private final RefreshStrategy delegate;
        private final Predicate<Long> condition;

        public ConditionalRefreshStrategy(RefreshStrategy delegate, Predicate<Long> condition) {
            this.delegate = delegate;
            this.condition = condition;
        }

        @Override
        public boolean needsRefresh(long secondsSinceLastAccess) {
            if (condition.apply(secondsSinceLastAccess)){
                return delegate.needsRefresh(secondsSinceLastAccess);
            }
            return false;
        }

        @Override
        public void refreshed() {
            delegate.refreshed();
        }
    }

}
