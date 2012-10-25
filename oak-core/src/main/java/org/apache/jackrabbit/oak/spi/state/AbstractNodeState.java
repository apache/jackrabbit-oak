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
package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.PropertyState;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

/**
 * Abstract base class for {@link NodeState} implementations.
 * This base class contains default implementations of the
 * {@link #equals(Object)} and {@link #hashCode()} methods based on
 * the implemented interface.
 * <p>
 * This class also implements trivial (and potentially very slow) versions of
 * the {@link #getProperty(String)} and {@link #getPropertyCount()} methods
 * based on {@link #getProperties()}. The {@link #getChildNode(String)} and
 * {@link #getChildNodeCount()} methods are similarly implemented based on
 * {@link #getChildNodeEntries()}. Subclasses should normally
 * override these method with a more efficient alternatives.
 */
public abstract class AbstractNodeState implements NodeState {

    @Override
    public PropertyState getProperty(String name) {
        for (PropertyState property : getProperties()) {
            if (name.equals(property.getName())) {
                return property;
            }
        }
        return null;
    }

    @Override
    public long getPropertyCount() {
        return count(getProperties());
    }

    @Override
    public boolean hasChildNode(String name) {
        return getChildNode(name) != null;
    }

    @Override
    public NodeState getChildNode(String name) {
        for (ChildNodeEntry entry : getChildNodeEntries()) {
            if (name.equals(entry.getName())) {
                return entry.getNodeState();
            }
        }
        return null;
    }

    @Override
    public long getChildNodeCount() {
        return count(getChildNodeEntries());
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return Iterables.transform(
                getChildNodeEntries(),
                new Function<ChildNodeEntry, String>() {
                    @Override
                    public String apply(ChildNodeEntry input) {
                        return input.getName();
                    }
                });
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Iterables.transform(
                getChildNodeNames(),
                new Function<String, ChildNodeEntry>() {
                    @Override
                    public ChildNodeEntry apply(final String input) {
                        return new AbstractChildNodeEntry() {
                            @Override @Nonnull
                            public String getName() {
                                return input;
                            }
                            @Override @Nonnull
                            public NodeState getNodeState() {
                                return getChildNode(input);
                            }
                        };
                    }
                });
    }

    /**
     * Generic default comparison algorithm that simply walks through the
     * property and child node lists of the given base state and compares
     * the entries one by one with corresponding ones (if any) in this state.
     */
    @Override
    public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        Set<String> baseProperties = new HashSet<String>();
        for (PropertyState beforeProperty : base.getProperties()) {
            String name = beforeProperty.getName();
            PropertyState afterProperty = getProperty(name);
            if (afterProperty == null) {
                diff.propertyDeleted(beforeProperty);
            } else {
                baseProperties.add(name);
                if (!beforeProperty.equals(afterProperty)) {
                    diff.propertyChanged(beforeProperty, afterProperty);
                }
            }
        }
        for (PropertyState afterProperty : getProperties()) {
            if (!baseProperties.contains(afterProperty.getName())) {
                diff.propertyAdded(afterProperty);
            }
        }

        Set<String> baseChildNodes = new HashSet<String>();
        for (ChildNodeEntry beforeCNE : base.getChildNodeEntries()) {
            String name = beforeCNE.getName();
            NodeState beforeChild = beforeCNE.getNodeState();
            NodeState afterChild = getChildNode(name);
            if (afterChild == null) {
                diff.childNodeDeleted(name, beforeChild);
            } else {
                baseChildNodes.add(name);
                if (!beforeChild.equals(afterChild)) {
                    diff.childNodeChanged(name, beforeChild, afterChild);
                }
            }
        }
        for (ChildNodeEntry afterChild : getChildNodeEntries()) {
            String name = afterChild.getName();
            if (!baseChildNodes.contains(name)) {
                diff.childNodeAdded(name, afterChild.getNodeState());
            }
        }
    }

    /**
     * Returns a string representation of this child node entry.
     *
     * @return string representation
     */
    public String toString() {
        StringBuilder builder = new StringBuilder("{");
        AtomicBoolean first = new AtomicBoolean(true);
        for (PropertyState property : getProperties()) {
            if (!first.getAndSet(false)) {
                builder.append(',');
            }
            builder.append(' ').append(property);
        }
        for (ChildNodeEntry entry : getChildNodeEntries()) {
            if (!first.getAndSet(false)) {
                builder.append(',');
            }
            builder.append(' ').append(entry);
        }
        builder.append(" }");
        return builder.toString();
    }

    /**
     * Checks whether the given object is equal to this one. Two node states
     * are considered equal if all their properties and child nodes match,
     * regardless of ordering. Subclasses may override this method with a
     * more efficient equality check if one is available.
     *
     * @param that target of the comparison
     * @return {@code true} if the objects are equal,
     *         {@code false} otherwise
     */
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that == null || !(that instanceof NodeState)) {
            return false;
        }

        NodeState other = (NodeState) that;

        if (getPropertyCount() != other.getPropertyCount()
                || getChildNodeCount() != other.getChildNodeCount()) {
            return false;
        }

        for (PropertyState property : getProperties()) {
            if (!property.equals(other.getProperty(property.getName()))) {
                return false;
            }
        }

        // TODO inefficient unless there are very few child nodes
        for (ChildNodeEntry entry : getChildNodeEntries()) {
            if (!entry.getNodeState().equals(
                    other.getChildNode(entry.getName()))) {
                return false;
            }
        }

        return true;

    }

    /**
     * Returns a hash code that's compatible with how the
     * {@link #equals(Object)} method is implemented. The current
     * implementation simply returns zero for everything since
     * {@link NodeState} instances are not intended for use as hash keys.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return 0;
    }

    //-----------------------------------------------------------< private >--

    private static long count(Iterable<?> iterable) {
        long n = 0;
        Iterator<?> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            n++;
        }
        return n;
    }

}
