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
package org.apache.jackrabbit.oak.jcr.version;

import java.io.Closeable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.oak.spi.toggle.Feature.newFeature;

/**
 * Logger facility for frozen node lookups by identifier. Calls to
 * {@link #lookupById(Tree)} first check the feature toggle
 * {@code oak.logFrozenNodeLookup} and then whether the given {@code Tree} is of
 * type {@code nt:frozenNode} in which case the path of the tree is logged at
 * INFO. Log messages are rate limited to one per second. If multiple frozen
 * nodes are looked up by identifier with the period of one second, then only
 * the first lookup is logged; the other lookups are not logged.
 * Enabling DEBUG level for this class reveals the stack of the calling thread.
 */
public class FrozenNodeLogger implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FrozenNodeLogger.class);

    private static final long LOG_INTERVAL = Long.getLong("oak.frozenNodeLogger.minInterval", 1000);

    private static long NO_MESSAGE_UNTIL = 0;

    private final Clock clock;

    private final Feature feature;

    public FrozenNodeLogger(@NotNull Clock clock,
                            @NotNull Whiteboard whiteboard) {
        this.clock = checkNotNull(clock);
        this.feature = newFeature("OAK-9139_log_frozen_node_lookup", whiteboard);
    }

    public final void lookupById(@NotNull Tree tree) {
        if (!feature.isEnabled()) {
            return;
        }
        PropertyState primaryType = tree.getProperty(JCR_PRIMARYTYPE);
        if (primaryType != null
                && !primaryType.isArray()
                && primaryType.getValue(Type.STRING).equals(NT_FROZENNODE)) {
            long now = clock.getTime();
            synchronized (FrozenNodeLogger.class) {
                if (now >= NO_MESSAGE_UNTIL) {
                    NO_MESSAGE_UNTIL = now + LOG_INTERVAL;
                    logFrozenNode(tree);
                }
            }
        }
    }

    @Override
    public void close() {
        feature.close();
    }

    protected void logFrozenNode(Tree tree) {
        log.info("Frozen node {} looked up by id", tree.getPath());
        if (log.isDebugEnabled()) {
            log.debug("Frozen node lookup call stack", new Exception());
        }
    }
}
