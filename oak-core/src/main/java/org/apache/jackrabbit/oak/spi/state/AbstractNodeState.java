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

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * Abstract base class for {@link NodeState} implementations.
 * This base class contains default implementations of the
 * {@link #equals(Object)} and {@link #hashCode()} methods based on
 * the implemented interface.
 * <p>
 * This class also implements trivial (and potentially very slow) versions of
 * the {@link #getProperty(String)} and {@link #getPropertyCount()} methods
 * based on {@link #getProperties()}. The {@link #getChildNodeCount()} method
 * is similarly implemented based on {@link #getChildNodeEntries()}.
 * Subclasses should normally override these method with a more efficient
 * alternatives.
 */
public abstract class AbstractNodeState implements NodeState {

    @Override
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public boolean getBoolean(String name) {
        PropertyState property = getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    @Override @CheckForNull
    public String getName(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @Override @Nonnull
    public Iterable<String> getNames(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

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
        return getChildNode(name).exists();
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

    /**
     * Generic default comparison algorithm that simply walks through the
     * property and child node lists of the given base state and compares
     * the entries one by one with corresponding ones (if any) in this state.
     */
    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (!comparePropertiesAgainstBaseState(base, diff)) {
            return false;
        }

        Set<String> baseChildNodes = new HashSet<String>();
        for (ChildNodeEntry beforeCNE : base.getChildNodeEntries()) {
            String name = beforeCNE.getName();
            NodeState beforeChild = beforeCNE.getNodeState();
            NodeState afterChild = getChildNode(name);
            if (!afterChild.exists()) {
                if (!diff.childNodeDeleted(name, beforeChild)) {
                    return false;
                }
            } else {
                baseChildNodes.add(name);
                if (!beforeChild.equals(afterChild)) {
                    if (!diff.childNodeChanged(name, beforeChild, afterChild)) {
                        return false;
                    }
                }
            }
        }

        for (ChildNodeEntry afterChild : getChildNodeEntries()) {
            String name = afterChild.getName();
            if (!baseChildNodes.contains(name)) {
                if (!diff.childNodeAdded(name, afterChild.getNodeState())) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Returns a string representation of this node state.
     *
     * @return string representation
     */
    public String toString() {
        if (!exists()) {
            return "{N/A}";
        }
        StringBuilder builder = new StringBuilder("{");
        String separator = " ";
        for (PropertyState property : getProperties()) {
            builder.append(separator);
            separator = ", ";
            builder.append(property);
        }
        for (ChildNodeEntry entry : getChildNodeEntries()) {
            builder.append(separator);
            separator = ", ";
            builder.append(entry);
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

        if (exists() != other.exists()) {
            return false;
        }

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

    /**
     * Compares the properties of {@code base} state with {@code this}
     * state.
     *
     * @param base the base node state.
     * @param diff the node state diff.
     */
    protected boolean comparePropertiesAgainstBaseState(NodeState base,
                                                     NodeStateDiff diff) {
        Set<String> baseProperties = new HashSet<String>();
        for (PropertyState beforeProperty : base.getProperties()) {
            String name = beforeProperty.getName();
            PropertyState afterProperty = getProperty(name);
            if (afterProperty == null) {
                if (!diff.propertyDeleted(beforeProperty)) {
                    return false;
                }
            } else {
                baseProperties.add(name);
                if (!beforeProperty.equals(afterProperty)) {
                    if (!diff.propertyChanged(beforeProperty, afterProperty)) {
                        return false;
                    }
                }
            }
        }

        for (PropertyState afterProperty : getProperties()) {
            if (!baseProperties.contains(afterProperty.getName())) {
                if (!diff.propertyAdded(afterProperty)) {
                    return false;
                }
            }
        }

        return true;
    }

    //-----------------------------------------------------------< private >--

    protected static long count(Iterable<?> iterable) {
        long n = 0;
        Iterator<?> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            n++;
        }
        return n;
    }

}
