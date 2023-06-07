/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.segment.azure.util;

import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RetrierTest {

    @Test
    public void succeedAtFirstAttempt() throws IOException {
        Retrier retrier = Retrier.withParams(10, 1);
        RetryTester execution = new RetryTester(1, "OK");

        String result = retrier.execute(execution);

        assertEquals("OK", result);
        assertEquals(1, execution.attempts);
    }

    @Test
    public void succeedAfterSeveralRetries() throws IOException {
        Retrier retrier = Retrier.withParams(10, 1);
        RetryTester execution = new RetryTester(5, "OK");

        String result = retrier.execute(execution);

        assertEquals("OK", result);
        assertEquals(5, execution.attempts);
    }

    @Test
    public void failAfterMaxRetries() {
        Retrier retrier = Retrier.withParams(5, 1);
        RetryTester execution = new RetryTester(10, "OK");

        assertThrows(IOException.class, () -> retrier.execute(execution));
        assertEquals(5, execution.attempts);
    }

    @Test
    public void retryOnExpectedExceptions() {
        Retrier retrier = Retrier.withParams(5, 1);

        RetryTester throwingIOException = new RetryTester(10, "OK");
        assertThrows(IOException.class, () -> retrier.execute(throwingIOException));
        assertEquals(5, throwingIOException.attempts);

        RetryTester throwingRepoNotReachableException = new RetryTester(10, "OK", () ->
                new RepositoryNotReachableException(new RuntimeException()));
        assertThrows(RepositoryNotReachableException.class, () -> retrier.execute(throwingRepoNotReachableException));
        assertEquals(5, throwingRepoNotReachableException.attempts);
    }

    @Test
    public void failWithoutRetryingOnUnexpectedExceptions() {
        Retrier retrier = Retrier.withParams(5, 1);
        RetryTester execution = new RetryTester(10, "OK", NullPointerException::new);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> retrier.execute(execution));
        assertEquals("Unexpected exception while executing the operation", ex.getMessage());
        assertEquals(NullPointerException.class, ex.getCause().getClass());
        assertEquals(1, execution.attempts);
    }

    @Test
    public void executeRunnable() throws IOException {
        Retrier retrier = Retrier.withParams(10, 1);
        RetryTesterRunnable execution = new RetryTesterRunnable(5);

        retrier.execute(execution);

        assertEquals(5, execution.attempts);
    }

    private static class RetryTester implements Retrier.ThrowingSupplier<String> {
        private final int succeedAfterAttempts;
        private final String result;
        private final Supplier<RuntimeException> runtimeExceptionSupplier;

        private int attempts = 0;

        public RetryTester(int succeedAfterAttempts, String result, Supplier<RuntimeException> runtimeExceptionSupplier) {
            this.succeedAfterAttempts = succeedAfterAttempts;
            this.result = result;
            this.runtimeExceptionSupplier = runtimeExceptionSupplier;
        }

        public RetryTester(int succeedAfterAttempts, String result) {
            this(succeedAfterAttempts, result, () -> null);
        }

        @Override
        public String get() throws IOException {
            attempts++;
            if (attempts == succeedAfterAttempts) {
                return result;
            }
            RuntimeException runtimeException1 = runtimeExceptionSupplier.get();
            if (runtimeException1 != null) {
                throw runtimeException1;
            }
            throw new IOException("Fail at attempt " + attempts);
        }
    }

    private static class RetryTesterRunnable implements Retrier.ThrowingRunnable {
        private final int succeedAfterAttempts;

        private int attempts = 0;

        public RetryTesterRunnable(int succeedAfterAttempts) {
            this.succeedAfterAttempts = succeedAfterAttempts;
        }

        @Override
        public void run() throws IOException {
            attempts++;
            if (attempts == succeedAfterAttempts) {
                return;
            }
            throw new IOException("Fail at attempt " + attempts);
        }
    }
}