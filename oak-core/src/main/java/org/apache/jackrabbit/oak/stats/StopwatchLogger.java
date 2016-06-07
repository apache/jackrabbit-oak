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
package org.apache.jackrabbit.oak.stats;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to be used for tracking of timing within methods. It makes use of the
 * {@link Clock.Fast} for speeding up the operation.
 */
public class StopwatchLogger implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(StopwatchLogger.class);

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final String clazz;
    
    private Clock clock;
    private Logger customLog;
    
    private long start;
    
    /**
     * Create a class with the provided class.
     * 
     * @param clazz
     */
    public StopwatchLogger(@Nonnull final String clazz) {
        this(null, checkNotNull(clazz));
    }

    /**
     * instantiate a class with the provided class
     * 
     * @param clazz
     */
    public StopwatchLogger(@Nonnull final Class<?> clazz) {
        this(checkNotNull(clazz).getName().toString());
    }

    /**
     * Instantiate a class with the provided class and custom logger. The provided logger, if not
     * null, will be then used for tracking down times
     * 
     * @param customLog
     * @param clazz
     */
    public StopwatchLogger(@Nullable final Logger customLog, @Nonnull final Class<?> clazz) {
        this(customLog, checkNotNull(clazz).getName().toString());
    }

    /**
     * Instantiate a class with the provided class and custom logger. The provided logger, if not
     * null, will be then used for tracking down times
     *
     * @param customLog
     * @param clazz
     */
    public StopwatchLogger(@Nullable final Logger customLog, @Nonnull final String clazz) {
        this.clazz = checkNotNull(clazz);
        this.customLog = customLog;
    }

    /**
     * starts the clock
     */
    public void start() {
        clock = new Clock.Fast(executor);
        start = clock.getTimeMonotonic();
    }
    
    /**
     * track of an intermediate time without stopping the ticking.
     * 
     * @param message
     */
    public void split(@Nullable final String message) {
        track(this, message);
    }
    
    /**
     * track the time and stop the clock.
     * 
     * @param message
     */
    public void stop(@Nullable final String message) {
        track(this, message);
        clock = null;
    }

    /**
     * convenience method for tracking the messages
     * 
     * @param customLog a potential custom logger. If null the static instance will be used
     * @param clock the clock used for tracking.
     * @param clazz the class to be used during the tracking of times
     * @param message a custom message for the tracking.
     */
    private static void track(@Nonnull final StopwatchLogger swl,
                              @Nullable final String message) {

        checkNotNull(swl);
        
        if (swl.isEnabled()) {
            Logger l = swl.getLogger();
            
            if (swl.clock == null) {
                l.debug("{} - clock has not been started yet.", swl.clazz);
            } else {
                Clock c = swl.clock;
                
                l.debug(
                    "{} - {} {}ms",
                    new Object[] { checkNotNull(swl.clazz), message == null ? "" : message,
                                  c.getTimeMonotonic() - swl.start});
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            executor.shutdownNow();            
        } catch (Throwable t) {
            LOG.error("Error while shutting down the scheduler.", t);
        }
    }
    
    /**
     * @return true if the clock has been started. False otherwise.
     */
    public boolean isStarted() {
        return clock != null;
    }
    
    private Logger getLogger() {
        return (customLog == null) ? LOG :  customLog;
    }
    
    /**
     * @return true whether the provided appender has DEBUG enabled and therefore asked to track
     *         times.
     */
    public boolean isEnabled() {
        return getLogger().isDebugEnabled();
    }
}
