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
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public final class TimedCommandListener implements com.mongodb.event.CommandListener {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class);
    private final String threadName;
    private final Stopwatch clock = Stopwatch.createUnstarted();

    public TimedCommandListener(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        if (isMongoDumpThread() && LOG.isDebugEnabled()) {
            String elapsed;
            if (clock.isRunning()) {
                elapsed = Long.toString(clock.elapsed(TimeUnit.MILLISECONDS));
            } else {
                elapsed = "N/A";
            }
            clock.reset().start();
            LOG.debug("Command started: {}, processing time: {}", event.getCommand(), elapsed);
        }
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        if (isMongoDumpThread() && LOG.isDebugEnabled()) {
            LOG.debug("Command succeeded: {}, Time: {} ms, client time: {}",
                    event.getCommandName(),
                    event.getElapsedTime(TimeUnit.MILLISECONDS),
                    clock.elapsed(TimeUnit.MILLISECONDS)
            );
            clock.reset().start();
        }
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        if (isMongoDumpThread()) {
            LOG.debug("Command failed: {}", event);
        }
    }

    private boolean isMongoDumpThread() {
        return Thread.currentThread().getName().equals(threadName);
    }
}
