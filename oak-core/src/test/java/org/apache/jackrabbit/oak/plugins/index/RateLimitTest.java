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
package org.apache.jackrabbit.oak.plugins.index;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

public class RateLimitTest {

    @Test
    public void rateLimitNoDelay() {
        Instant now = Instant.now();
        // reset the rate limit
        RateLimitUtils.rateLimit(now, Duration.ofSeconds(0), false);
        for (int i = 0; i < 30; i++) {
            assertEquals(0, RateLimitUtils.rateLimit(now, Duration.ofSeconds(i), false));
            now = now.plus(Duration.ofSeconds(1));
        }
    }

    @Test
    public void rateLimitDelay() {
        Instant now = Instant.now();
        // reset the rate limit
        RateLimitUtils.rateLimit(now, Duration.ofSeconds(0), false);
        now = now.plus(Duration.ofSeconds(1));
        for (int i = 30; i < 60; i++) {
            long sleep = RateLimitUtils.rateLimit(now, Duration.ofSeconds(i), false);
            assertEquals(1000, sleep);
            now = now.plus(Duration.ofMillis(sleep));
            now = now.plus(Duration.ofSeconds(1));
        }        
    }
    
    @Test
    public void rateLimitMaxOneMinute() {
        Instant now = Instant.now();
        // reset the rate limit
        RateLimitUtils.rateLimit(now, Duration.ofSeconds(0), false);
        now = now.plus(Duration.ofMinutes(1));
        for (int i = 60; i < 120; i++) {
            long sleep = RateLimitUtils.rateLimit(now, Duration.ofMinutes(i), false);
            assertEquals(60 * 1000, sleep);
            now = now.plus(Duration.ofMillis(sleep));
            now = now.plus(Duration.ofMinutes(10));
        }        
    }
    
}
