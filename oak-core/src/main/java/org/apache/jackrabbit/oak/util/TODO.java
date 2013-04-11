/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.util;

import java.util.concurrent.Callable;

import javax.jcr.UnsupportedRepositoryOperationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for identifying partially implemented features and
 * controlling their runtime behavior.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-193">OAK-193</a>
 */
public class TODO {

    private static final String mode = System.getProperty("todo", "strict");

    private static boolean strict = "strict".equals(mode);

    private static boolean log = "log".equals(mode);

    public static void relax() {
        strict = false;
        log = true;
    }

    public static TODO unimplemented() {
        return new TODO("unimplemented");
    }

    public static TODO dummyImplementation() {
        return new TODO("dummy implementation");
    }

    private final UnsupportedOperationException exception;

    private final Logger logger;

    private final String message;

    private TODO(String message) {
        this.exception = new UnsupportedOperationException(message);
        StackTraceElement[] trace = exception.getStackTrace();
        if (trace != null && trace.length > 2) {
            String className = trace[2].getClassName();
            String methodName = trace[2].getMethodName();
            this.logger = LoggerFactory.getLogger(className);
            this.message =
                    "TODO: " + className + "." + methodName + "() - " + message;
        } else {
            this.logger = LoggerFactory.getLogger(TODO.class);
            this.message = "TODO: " + message;
        }
    }

    public <T> T returnValueOrNull(T value) {
        if (strict) {
            return null;
        } else {
            if (log) {
                logger.warn(message, exception);
            }
            return value;
        }
    }

    public void doNothing() throws UnsupportedRepositoryOperationException {
        if (strict) {
            throw exception();
        } else if (log) {
            logger.warn(message, exception);
        }
    }

    public UnsupportedRepositoryOperationException exception() {
        return new UnsupportedRepositoryOperationException(message, exception);
    }

    public <T> T returnValue(final T value)
            throws UnsupportedRepositoryOperationException {
        return call(new Callable<T>() {
            @Override
            public T call() {
                return value;
            }
        });
    }

    public <T> T call(Callable<T> callable)
            throws UnsupportedRepositoryOperationException {
        if (strict) {
            throw exception();
        } else if (log) {
            logger.warn(message, exception);
        }
        try {
            return callable.call();
        } catch (Exception e) {
            throw new UnsupportedRepositoryOperationException(
                    message + " failure", e);
        }
    }

}
