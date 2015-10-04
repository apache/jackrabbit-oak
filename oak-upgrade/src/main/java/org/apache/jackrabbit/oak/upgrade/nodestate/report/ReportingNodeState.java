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
package org.apache.jackrabbit.oak.upgrade.nodestate.report;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;

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
 * @see Reporter, PeriodicReporter, LoggingReporter
 */
public class ReportingNodeState implements NodeState {

    private final ReportingNodeState parent;
    private final NodeState delegate;
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

    protected NodeState wrapChild(String name, NodeState delegate) {
        return wrapAndReport(this, name, delegate, this.reporter);
    }

    private ReportingNodeState(ReportingNodeState parent, String name, NodeState delegate, Reporter reporter) {
        this.parent = parent;
        this.delegate = delegate;
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
        return PathUtils.concat(parent.getPath(), name);
    }

    /**
     * The ReportingNodeState implementation returns a ReadOnlyBuilder, which
     * will fail for any mutable operation.
     *
     * @return a NodeBuilder instance corresponding to this NodeState.
     */
    @Override
    @Nonnull
    public NodeBuilder builder() {
        return new ReadOnlyBuilder(this);
    }

    @Override
    public boolean compareAgainstBaseState(final NodeState base, final NodeStateDiff diff) {
        return delegate.compareAgainstBaseState(base, new ReportingDiff(diff, this));
    }

    /**
     * Note that any implementation-specific optimizations of wrapped NodeStates
     * will not work if a ReportingNodeState is passed into their {@code #equals()}
     * method. This implementation will compare the wrapped NodeState, however. So
     * optimizations work when calling {@code #equals()} on a ReportingNodeState.
     *
     * @param other Object to compare with this NodeState.
     * @return true if the given object is equal to this NodeState, false otherwise.
     */
    @Override
    public boolean equals(final Object other) {
        if (other instanceof ReportingNodeState) {
            return delegate.equals(((ReportingNodeState) other).delegate);
        } else {
            return delegate.equals(other);
        }
    }

    @Override
    public boolean exists() {
        return delegate.exists();
    }

    @Override
    public boolean hasChildNode(@Nonnull final String name) {
        return delegate.hasChildNode(name);
    }

    @Override
    @Nonnull
    public NodeState getChildNode(@Nonnull final String name) throws IllegalArgumentException {
        return wrapChild(name, delegate.getChildNode(name));
    }

    @Override
    public long getChildNodeCount(final long max) {
        return delegate.getChildNodeCount(max);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return delegate.getChildNodeNames();
    }

    @Override
    @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Iterables.transform(
                delegate.getChildNodeEntries(),
                new Function<ChildNodeEntry, ChildNodeEntry>() {
                    @Override
                    @Nonnull
                    public ChildNodeEntry apply(final ChildNodeEntry childNodeEntry) {
                        final String name = childNodeEntry.getName();
                        return new MemoryChildNodeEntry(name, wrapChild(name, childNodeEntry.getNodeState()));
                    }
                }
        );
    }

    @Override
    public boolean hasProperty(@Nonnull final String name) {
        return delegate.hasProperty(name);
    }

    @Override
    public boolean getBoolean(@Nonnull final String name) {
        reporter.reportProperty(this, name);
        return delegate.getBoolean(name);
    }

    @Override
    public long getLong(final String name) {
        reporter.reportProperty(this, name);
        return delegate.getLong(name);
    }

    @Override
    @CheckForNull
    public String getName(@Nonnull final String name) {
        reporter.reportProperty(this, name);
        return delegate.getName(name);
    }

    @Override
    @Nonnull
    public Iterable<String> getNames(@Nonnull final String name) {
        reporter.reportProperty(this, name);
        return delegate.getNames(name);
    }


    @Override
    @CheckForNull
    public String getString(final String name) {
        reporter.reportProperty(this, name);
        return delegate.getString(name);
    }

    @Override
    @Nonnull
    public Iterable<String> getStrings(@Nonnull final String name) {
        reporter.reportProperty(this, name);
        return delegate.getStrings(name);
    }

    @Override
    @CheckForNull
    public PropertyState getProperty(@Nonnull final String name) {
        reporter.reportProperty(this, name);
        return delegate.getProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return delegate.getPropertyCount();
    }

    @Override
    @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(
                delegate.getProperties(),
                new Function<PropertyState, PropertyState>() {
                    @Override
                    @Nonnull
                    public PropertyState apply(final PropertyState propertyState) {
                        reporter.reportProperty(ReportingNodeState.this, propertyState.getName());
                        return propertyState;
                    }
                }
        );
    }

    @Override
    public String toString() {
        return "ReportingNodeState{" + getPath() + ", " + super.toString() + "}";
    }

    private static class ReportingDiff implements NodeStateDiff {

        private final NodeStateDiff diff;
        private ReportingNodeState parent;

        public ReportingDiff(NodeStateDiff diff, ReportingNodeState parent) {
            this.diff = diff;
            this.parent = parent;
        }

        @Override
        public boolean childNodeAdded(final String name, final NodeState after) {
            return diff.childNodeAdded(name, parent.wrapChild(name, after));
        }

        @Override
        public boolean childNodeChanged(final String name, final NodeState before, final NodeState after) {
            return diff.childNodeChanged(name, before, parent.wrapChild(name, after));
        }

        @Override
        public boolean childNodeDeleted(final String name, final NodeState before) {
            return diff.childNodeDeleted(name, before);
        }

        @Override
        public boolean propertyAdded(final PropertyState after) {
            return diff.propertyAdded(after);
        }

        @Override
        public boolean propertyChanged(final PropertyState before, final PropertyState after) {
            return diff.propertyChanged(before, after);
        }

        @Override
        public boolean propertyDeleted(final PropertyState before) {
            return diff.propertyDeleted(before);
        }
    }
}
