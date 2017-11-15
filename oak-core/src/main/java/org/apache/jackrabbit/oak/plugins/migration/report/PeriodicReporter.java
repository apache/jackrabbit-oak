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
package org.apache.jackrabbit.oak.plugins.migration.report;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract class that simplifies development of a Reporter
 * that should only report every nth event (node or property seen).
 */
public abstract class PeriodicReporter implements Reporter {

    private final int nodeLogInterval;

    private final int propertyLogInterval;

    private AtomicLong nodes = new AtomicLong(0);

    private AtomicLong properties = new AtomicLong(0);

    protected PeriodicReporter(final int nodeLogInterval, final int propertyLogInterval) {
        this.nodeLogInterval = nodeLogInterval;
        this.propertyLogInterval = propertyLogInterval;
    }

    /**
     * Reset the node and property counts to 0. Inheriting implementations
     * may reset their own internal state.
     */
    protected void reset() {
        nodes.set(0);
        properties.set(0);
    }

    /**
     * Callback called every nth time a node is accessed.
     *
     * @param count The count of reported nodes.
     * @param nodeState The node that was reported.
     */
    protected abstract void reportPeriodicNode(
            final long count, @Nonnull final ReportingNodeState nodeState);

    /**
     * Callback called every nth time a property is accessed.
     *
     * @param count The count of reported properties.
     * @param parent The parent node of the reported property.
     * @param propertyName The name of the reported property.
     */
    protected abstract void reportPeriodicProperty(
            final long count, @Nonnull final ReportingNodeState parent, @Nonnull final String propertyName);


    protected boolean skipNodeState(@Nonnull final ReportingNodeState nodeState) {
        return false;
    }

    @Override
    public final void reportNode(@Nonnull final ReportingNodeState nodeState) {
        if (nodeLogInterval == -1) {
            return;
        }

        if (skipNodeState(nodeState)) {
            return;
        }

        final long count = nodes.incrementAndGet();
        if (count % nodeLogInterval == 0) {
            reportPeriodicNode(count, nodeState);
        }
    }

    @Override
    public final void reportProperty(@Nonnull final ReportingNodeState parent, @Nonnull final String propertyName) {
        if (propertyLogInterval == -1) {
            return;
        }

        final long count = properties.incrementAndGet();
        if (count % propertyLogInterval == 0) {
            reportPeriodicProperty(count, parent, propertyName);
        }
    }
}
