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

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RateLimitUtils {
    
    private final static Logger LOG = LoggerFactory.getLogger(RateLimitUtils.class);    

    // when getOldestAsyncIndexUpdate was called the last time 
    private static Instant lastJmxBeanCall = Instant.now();

    // the return value of the last call to getOldestAsyncIndexUpdate
    private static Instant cachedOldestAsyncUpdate = Instant.now();
    
    // the last time the rateLimitWrites was called
    private static Instant lastRateLimitCall = Instant.now();

    private RateLimitUtils() {
    }
    
    /**
     * Rate limit writes in case indexes are lagging behind too far. The method
     * returns immediately if all async indexes are up-to-date (updated in the last
     * 30 seconds).
     * 
     * If indexing lanes are lagging behind, however, the method will wait (using
     * Thread.sleep) for at most 1 minute. If the method is called more than once
     * per minute, it will sleep for at most the time that passed until the last
     * call; that is, an application that is calling it a lot will be paused for up
     * to 50%. This assumes indexes will be able to catch up in this situation.
     * 
     * @return the number of milliseconds the call was sleeping
     */
    public static long rateLimitWrites() {
        Instant now = Instant.now();
        Duration delay = Duration.between(getOldestAsyncIndexUpdate(now), now);
        return rateLimit(now, delay, true);
    }
    
    static long rateLimit(Instant now, Duration delay, boolean callThreadSleep) {
        long sleep;
        if (delay.getSeconds()  < 30) {
            // less than 30 seconds: no need to wait
            sleep = 0;
        } else {
            if (delay.toMinutes() > 60) {
                LOG.warn("Indexing is delayed for {} minutes", delay.toMinutes());
            }
            // sleep for as long as the duration between the last call and this call
            sleep = Duration.between(lastRateLimitCall, now).toMillis();
            // sleep at most 1 minute
            sleep = Math.min(sleep, Duration.ofMinutes(1).toMillis());
            // ensure we don't try to sleep negative values (it would throw IllegalArgumentException)
            sleep = Math.max(0, sleep);
            if (callThreadSleep) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            now = now.plus(Duration.ofMillis(sleep));
        }
        lastRateLimitCall = now;
        return sleep;
    }
    
    /**
     * Get the timestamp of the oldest indexing lane. In the normal case, this is at
     * most 5 seconds ago, if indexing lanes are updated every 5 seconds. If
     * indexing is delayed, or paused, this can be in the past.
     * 
     * The method can be called often without issues. If the last call was less than
     * one second ago, the last returned value is returned. This is to avoid
     * unnecessary calls to the JMX beans.
     * 
     * @param now the current time
     * @return the timestamp of the oldest indexing lane
     */
    private static Instant getOldestAsyncIndexUpdate(Instant now) {
        if (Duration.between(lastJmxBeanCall, now).getSeconds() < 1) {
            return cachedOldestAsyncUpdate;
        }
        Instant oldestAsyncUpdate = now;
        MBeanServerConnection server = ManagementFactory.getPlatformMBeanServer();
        try {
            Set<ObjectName> beanNames;
            beanNames = server.queryNames(new ObjectName(
                "org.apache.jackrabbit.oak:type=IndexStats,*"), null);
            for (ObjectName objectName : beanNames) {
                Object time = server.getAttribute(objectName, "Done");
                if (time == null || time.toString().isEmpty()) {
                    time = server.getAttribute(objectName, "LastIndexedTime");
                }
                if (time != null && !time.toString().isEmpty()) {
                    Calendar cal = ISO8601.parse(time.toString());
                    Instant doneInstant = Instant.ofEpochMilli(cal.getTimeInMillis());
                    if (doneInstant.isBefore(oldestAsyncUpdate)) {
                        oldestAsyncUpdate = doneInstant;
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not retrieve the async lane state", e);
        }
        cachedOldestAsyncUpdate = oldestAsyncUpdate;
        return oldestAsyncUpdate;
    }

}
