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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Logs the commands executed by the Mongo client, including the time between a request and its response and between a
 * response and the next request (processing time). These metrics are useful to have an idea of the ratio between
 * time spent waiting for Mongo responses and time spent parsing and processing the responses.
 */
public final class MongoCommandListener implements CommandListener {
    private static final Logger LOG = LoggerFactory.getLogger(MongoCommandListener.class);
    private final String threadName;
    private final Stopwatch clock = Stopwatch.createUnstarted();

    /**
     * @param threadName the name of the thread that will be traced. Events from other threads will be ignored.
     */
    public MongoCommandListener(String threadName) {
        Preconditions.checkNotNull(threadName, "threadName cannot be null");
        this.threadName = threadName;
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        if (LOG.isTraceEnabled() && isMongoDumpThread()) {
            String elapsed;
            if (clock.isRunning()) {
                elapsed = clock.elapsed(TimeUnit.MILLISECONDS) + " ms";
            } else {
                elapsed = "N/A";
            }
            clock.reset().start();
            LOG.trace("Command started: {}. Processing time of previous response: {}", event.getCommand(), elapsed);
        }
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        if (LOG.isTraceEnabled() && isMongoDumpThread()) {
            LOG.trace("Command succeeded: {}, Time waiting for Mongo response: {} ms",
                    event.getCommandName(),
                    clock.elapsed(TimeUnit.MILLISECONDS)
            );
            clock.reset().start();
        }
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        if (LOG.isTraceEnabled() && isMongoDumpThread()) {
            LOG.trace("Command failed: {}", event);
        }
    }

    private boolean isMongoDumpThread() {
        return Thread.currentThread().getName().equals(threadName);
    }
}
