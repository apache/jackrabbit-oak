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
package org.apache.jackrabbit.mongomk.api.command;


/**
 * The {@code Command} framework provides an way to encapsulate specific actions
 * of the {code MicroKernel}.
 *
 * <p>
 * It adds some functionality for retries and other non business logic related
 * actions (i.e. logging, performance tracking, etc).
 * </p>
 *
 * @see <a href="http://en.wikipedia.org/wiki/Command_pattern">Command Pattern</a>
 * @see CommandExecutor
 *
 * @param <T> The result type of the {@code Command}.
 */
public interface Command<T> {

    /**
     * Executes the {@code Command} and returns its result.
     *
     * @return The result.
     * @throws Exception If an error occurred while executing.
     */
    T execute() throws Exception;

    /**
     * Returns the number of retries this {@code Command} should be retried in
     * case of an error or false result.
     *
     * <p>
     * The number of reties is evaluated in the following way:
     * <li>n < 0: Unlimited retries</li>
     * <li>n = 0: No retries (just one execution)</li>
     * <li>n > 0: Corresponding number of retries</li>
     * </p>
     *
     * <p>
     * In order to determine whether the {@code Command} should be retired on
     * {@link #needsRetry(Exception)} or {@link #needsRetry(Object)} will be called.
     * </p>
     *
     * @see #needsRetry(Exception)
     * @see #needsRetry(Object)
     *
     * @return The number of retries.
     */
    int getNumOfRetries();

    /**
     * Will be called in case of an {@link Exception} during the execution and
     * a given number of retries which has not exceeded.
     *
     * @param e The Exception which was thrown.
     * @return {@code true} if a retry should be performed, else {@code false}.
     */
    boolean needsRetry(Exception e);

    /**
     * Will be called in case of a successful execution and a given number of
     * retries which has not exceeded.
     *
     * <p>
     * This gives the implementor a chance to retry a false result.
     * </p>
     *
     * @param result The result of the execution.
     * @return {@code true} if a retry should be performed, else {@code false}.
     */
    boolean needsRetry(T result);
}