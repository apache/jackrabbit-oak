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
package org.apache.jackrabbit.oak.util;

import org.slf4j.Logger;

/**
 * PerfLogger is a simpler wrapper around a slf4j Logger which 
 * comes with the capability to issue log statements containing
 * the measurement between start() and end() methods.
 * <p>
 * Usage:
 * <ul>
 * <li>final long start = perflogger.start();</li>
 * <li>.. some code ..
 * <li>perflogger.end(start, 1, "myMethodName: param1={}", param1);</li>
 * </ul>
 * <p>
 * The above will do nothing if the log level for the logger passed
 * to PerfLogger at construction time is not DEBUG or TRACE - otherwise
 * start() will return the current time in milliseconds and end will
 * issue a log statement if the time between start and end was bigger
 * than 1 ms, and it will pass the parameters to the log statement.
 * The idea is to keep up performance at max possible if the log 
 * level is INFO or higher - but to allow some meaningful logging
 * if at DEBUG or TRACe. The difference between DEBUG and TRACE is
 * that TRACE will log start too (if a log message is passed to start)
 * and it will always log the end - whereas in case of DEBUG the start
 * will never be logged and the end will only be logged if the time
 * is bigger than what's passed to end.
 */
public final class PerfLogger {

    /** The logger to which the log statements are emitted **/
    private final Logger delegate;

    /** Create a new PerfLogger that shall use the given Logger object for logging **/
    public PerfLogger(Logger delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        this.delegate = delegate;
    }

    /**
     * Returns quickly if log level is not DEBUG or TRACE - otherwise just
     * returns the current time in millis.
     * 
     * @return the current time if level is DEBUG or TRACE, -1 otherwise
     */
    public final long start() {
        if (!delegate.isDebugEnabled()) {
            // if log level is not at least DEBUG, then return fast, no-op
            return -1;
        }
        return start(null);
    }

    /**
     * Returns quickly if log level is not DEBUG or TRACE - if it is DEBUG, then
     * just returns the current time in millis, if it is TRACE, then log the
     * given message and also return the current time in millis.
     * 
     * @param traceMsgOrNull
     *            the message to log if log level is TRACE - or null if no
     *            message should be logged (even on TRACE level)
     * @return the current time if level is DEBUG or TRACE, -1 otherwise
     */
    public final long start(String traceMsgOrNull) {
        if (!delegate.isDebugEnabled()) {
            // if log level is not at least DEBUG, then return fast, no-op
            return -1;
        }
        if (traceMsgOrNull != null && delegate.isTraceEnabled()) {
            delegate.trace(traceMsgOrNull);
        }
        return System.currentTimeMillis();
    }

    /**
     * Returns quickly if log level is not DEBUG or TRACE - if it is DEBUG
     * and the difference between now and the provided 'start' is bigger (slower)
     * than 'logAtDebugIfSlowerThanMs', then a log.debug is emitted, if at
     * TRACE then a log.trace is always emitted (independent of time measured).
     * <p>
     * Note that this method exists for performance optimization only (compared
     * to the other end() method with a vararg.
     * @param start the start time with which 'now' should be compared
     * @param logAtDebugIfSlowerThanMs the number of milliseconds that must
     * be surpassed to issue a log.debug (if log level is DEBUG)
     * @param logMessagePrefix the log message 'prefix' - to which the given
     * argument will be passed, plus the measured time difference in the format
     * '[took x ms']
     * @param arg1 the argument which is to be passed to the log statement
     */
    public final void end(long start, long logAtDebugIfSlowerThanMs,
            String logMessagePrefix, Object arg1) {
        if (start < 0) {
            return;
        }
        end(start, logAtDebugIfSlowerThanMs, logMessagePrefix,
                new Object[] { arg1 });
    }

    /**
     * Returns quickly if log level is not DEBUG or TRACE - if it is DEBUG
     * and the difference between now and the provided 'start' is bigger (slower)
     * than 'logAtDebugIfSlowerThanMs', then a log.debug is emitted, if at
     * TRACE then a log.trace is always emitted (independent of time measured).
     * <p>
     * Note that this method exists for performance optimization only (compared
     * to the other end() method with a vararg.
     * @param start the start time with which 'now' should be compared
     * @param logAtDebugIfSlowerThanMs the number of milliseconds that must
     * be surpassed to issue a log.debug (if log level is DEBUG)
     * @param logMessagePrefix the log message 'prefix' - to which the given
     * arguments will be passed, plus the measured time difference in the format
     * '[took x ms']
     * @param arg1 the first argument which is to be passed to the log statement
     * @param arg2 the second argument which is to be passed to the log statement
     */
    public final void end(long start, long logAtDebugIfSlowerThanMs,
            String logMessagePrefix, Object arg1, Object arg2) {
        if (start < 0) {
            return;
        }
        end(start, logAtDebugIfSlowerThanMs, logMessagePrefix, new Object[] {
                arg1, arg2 });
    }

    /**
     * Returns quickly if log level is not DEBUG or TRACE - if it is DEBUG
     * and the difference between now and the provided 'start' is bigger (slower)
     * than 'logAtDebugIfSlowerThanMs', then a log.debug is emitted, if at
     * TRACE then a log.trace is always emitted (independent of time measured).
     * @param start the start time with which 'now' should be compared
     * @param logAtDebugIfSlowerThanMs the number of milliseconds that must
     * be surpassed to issue a log.debug (if log level is DEBUG)
     * @param logMessagePrefix the log message 'prefix' - to which the given
     * arguments will be passed, plus the measured time difference in the format
     * '[took x ms']
     * @param arguments the arguments which is to be passed to the log statement
     */
    public void end(long start, long logAtDebugIfSlowerThanMs,
            String logMessagePrefix, Object... arguments) {
        if (start < 0) {
            // if start is negative, we assume that start() returned -1 because the log level is above DEBUG
            return;
        }

        final long diff = System.currentTimeMillis() - start;
        if (delegate.isTraceEnabled()) {
            // if log level is TRACE, then always log - and do that on TRACE
            // then:
            delegate.trace(logMessagePrefix + " [took " + diff + "ms]",
                    (Object[]) arguments);
        } else if ((logAtDebugIfSlowerThanMs < 0)
                || (diff > logAtDebugIfSlowerThanMs)) {
            // otherwise (log level is DEBUG) only log if
            // logDebugIfSlowerThanMs is set to -1 (or negative)
            // OR the measured diff is larger than logDebugIfSlowerThanMs -
            // and then do that on DEBUG:
            delegate.debug(logMessagePrefix + " [took " + diff + "ms]",
                    (Object[]) arguments);
        }
    }

    /** Whether or not the delegate has log level DEBUG configured **/
    public final boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    /** Whether or not the delegate has log level TRACE configured **/
    public final boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

}
