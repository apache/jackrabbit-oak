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
package org.apache.jackrabbit.oak.commons.conditions;

import java.util.Objects;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Validate {

    private Validate() {
        // no instances for you
    }

    private static final Logger LOG = LoggerFactory.getLogger(Validate.class);

    // when true, message template are arguments checked even when the condition
    // to check for is true
    private static final boolean CHECKMESSAGETEMPLATE = SystemPropertySupplier
            .create("oak.precondition.checks.CHECKMESSAGETEMPLATE", false).loggingTo(LOG).get();

    /**
     * Checks the specified expression
     * 
     * @param expression
     *            to check
     * @throws IllegalArgumentException
     *             when false
     */
    public static void checkArgument(boolean expression) throws IllegalArgumentException {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Checks the specified expression
     * 
     * @param expression
     *            to check
     * @param message
     *            to use in exception
     * @throws IllegalArgumentException
     *             when false
     */
    public static void checkArgument(boolean expression, @NotNull String message) throws IllegalArgumentException {

        Objects.requireNonNull(message);

        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks the specified expression
     * 
     * @param expression
     *            to checks
     * @param messageTemplate
     *            to use in exception (using {@link String#format} syntax
     * @throws IllegalArgumentException
     *             when false
     */
    public static void checkArgument(boolean expression, @NotNull String messageTemplate, @Nullable Object... messageArgs) {

        Objects.requireNonNull(messageTemplate);

        if (CHECKMESSAGETEMPLATE) {
            checkTemplate(messageTemplate, messageArgs);
        }

        if (!expression) {
            if (!CHECKMESSAGETEMPLATE) {
                checkTemplate(messageTemplate, messageArgs);
            }

            String message = String.format(messageTemplate, messageArgs);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks whether the specified expression is true
     *
     * @param expression expression to checks
     * @throws IllegalStateException if expression is false
     */
    public static void checkState(final boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    /**
     * Checks whether the specified expression is true
     *
     * @param expression expression to checks
     * @param errorMessage message to use in exception
     * @throws IllegalStateException if expression is false
     */
    public static void checkState(final boolean expression, @NotNull final Object errorMessage) {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    /**
     * Checks whether the specified expression is true
     *
     * @param expression expression to checks
     * @param messageTemplate to use in exception (using {@link String#format} syntax
     * @param messageArgs arguments to use in messageTemplate
     * @throws IllegalArgumentException if expression is false
     */
    public static void checkState(final boolean expression, @NotNull String messageTemplate, @Nullable Object... messageArgs) {

        Objects.requireNonNull(messageTemplate);

        if (CHECKMESSAGETEMPLATE) {
            checkTemplate(messageTemplate, messageArgs);
        }

        if (!expression) {
            if (!CHECKMESSAGETEMPLATE) {
                checkTemplate(messageTemplate, messageArgs);
            }

            String message = String.format(messageTemplate, messageArgs);
            throw new IllegalStateException(message);
        }
    }

    // helper methods

    static boolean checkTemplate(@NotNull String messageTemplate, @Nullable Object... messageArgs) {
        int argsSpecified = messageArgs.length;
        int argsInTemplate = countArguments(messageTemplate);
        boolean result = argsSpecified == argsInTemplate;
        if (argsSpecified != argsInTemplate) {
            LOG.error("Invalid message format: template '{}', argument count {}", messageTemplate, argsSpecified);
        }
        return result;
    }

    static int countArguments(String template) {
        int count = 0;
        boolean inEscape = false;

        for (char c : template.toCharArray()) {
            if (inEscape) {
                if (c != '%') {
                    count += 1;
                }
                inEscape = false;
            } else {
                if (c == '%') {
                    inEscape = true;
                }
            }
        }

        if (inEscape) {
            LOG.error("trailing escape character '%' found", new Exception("call stack"));
        }

        return count;
    }
}
