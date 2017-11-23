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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.migration.AbstractDecoratedNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A decoration layer for NodeState instances that intercepts
 * all accesses to NodeStates and PropertyStates (getters) and
 * informs a {@link Reporter} via its callbacks that the respective
 * NodeStates or PropertyStates have been accessed.
 * <br>
 * The decoration is deep, i.e. any child NodeStates will be
 * decorated as well and will report to the same {@code Reporter}
 * instance.
 * <br>
 * For convenience, a {@link PeriodicReporter} abstract class exists.
 * This simplifies reporting every nth node/property only.
 * <br>
 * Note: Multiple accesses to the same node or property are each
 * reported. Therefore if exactly counting unique accesses is a
 * requirement, the reporter needs to take care of de-duplication.
 *
 * @see Reporter
 * @see PeriodicReporter
 * @see LoggingReporter
 */
public class ReportingNodeState extends AbstractDecoratedNodeState {

    private final ReportingNodeState parent;
    private final String name;
    private final Reporter reporter;

    /**
     * Allows wrapping a NodeState as a ReportingNodeState. The wrapped
     * NodeState is treated as the root of a tree (i.e. path is "/").
     * <br>
     * Any children accessed via this NodeState are also wrapped. Each
     * wrapped NodeState is also reported to the provided Reporter.
     *
     * @param nodeState The NodeState to be wrapped.
     * @param reporter The reporter to report to.
     * @return the wrapped NodeState.
     */
    public static NodeState wrap(NodeState nodeState, Reporter reporter) {
        return wrapAndReport(null, "/", nodeState, reporter);
    }

    private static NodeState wrapAndReport(@Nullable ReportingNodeState parent, @Nonnull String name,
                                           @Nonnull NodeState delegate, @Nonnull Reporter reporter) {
        final ReportingNodeState nodeState = new ReportingNodeState(parent, name, delegate, reporter);
        reporter.reportNode(nodeState);
        return nodeState;
    }

    private ReportingNodeState(ReportingNodeState parent, String name, NodeState delegate, Reporter reporter) {
        super(delegate);
        this.parent = parent;
        this.name = name;
        this.reporter = reporter;
    }

    /**
     * ReportingNodeState instances provide access to their path via their
     * parent hierarchy. Note that calculating the path on every access may
     * incur a significant performance penalty.
     *
     * @return The path of the ReportingNodeState instance, assuming that
     *         the first wrapped instance is the root node.
     */
    public String getPath() {
        if (parent == null) {
            return name;
        }
        return PathUtils.concat(this.parent.getPath(), name);
    }

    @Nonnull
    @Override
    protected NodeState decorateChild(@Nonnull final String name, @Nonnull final NodeState delegateChild) {
        return wrapAndReport(this, name, delegateChild, reporter);
    }

    @Override
    @CheckForNull
    protected PropertyState decorateProperty(@Nonnull final PropertyState delegatePropertyState) {
        reporter.reportProperty(this, delegatePropertyState.getName());
        return delegatePropertyState;
    }

    @Override
    public String toString() {
        return "ReportingNodeState{" + getPath() + ", " + delegate.toString() + "}";
    }
}
