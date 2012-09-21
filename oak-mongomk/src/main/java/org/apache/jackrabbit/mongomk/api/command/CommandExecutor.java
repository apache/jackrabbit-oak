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
 * The executor part of the <a href="http://en.wikipedia.org/wiki/Command_pattern">Command Pattern</a>.
 *
 * <p>
 * The implementation of this class contains the business logic to execute a command.
 * </p>
 *
 * @see <a href="http://en.wikipedia.org/wiki/Command_pattern">Command Pattern</a>
 * @see Command
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public interface CommandExecutor {

    /**
     * Executes the given {@link Command} and returns the result.
     *
     * <p>
     * If an retry behavior is specified this will be taken care of by the implementation as well.
     * </p>
     *
     * @param command
     * @return The result of the execution.
     * @throws Exception If an error occurred while executing.
     */
    <T> T execute(Command<T> command) throws Exception;
}
