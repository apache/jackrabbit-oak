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
package org.apache.jackrabbit.oak.plugins.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static void assertEventually(Runnable r, long timeoutMillis) {
        final long start = System.currentTimeMillis();
        long lastAttempt = 0;
        int attempts = 0;

        while (true) {
            try {
                attempts++;
                LOG.info("assertEventually attempt count:{}", attempts);
                lastAttempt = System.currentTimeMillis();
                r.run();
                return;
            } catch (Throwable e) {
                long elapsedTime = lastAttempt - start;
                LOG.trace("assertEventually attempt {} failed because of {}", attempts, e.getMessage());
                if (elapsedTime >= timeoutMillis) {
                    String msg = String.format("Condition not satisfied after %1.2f seconds and %d attempts",
                            elapsedTime / 1000d, attempts);
                    throw new AssertionError(msg, e);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

            }
        }
    }
}
