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

import static java.lang.Integer.getInteger;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * Abstract base class for {@link NodeState} implementations.
 * This base class contains default implementations of the
 * {@link #equals(Object)} and {@link #hashCode()} methods based on
 * the implemented interface.
 * <p>
 * This class also implements trivial (and potentially very slow) versions of
 * the {@link #getProperty(String)} and {@link #getPropertyCount()} methods
 * based on {@link #getProperties()}. The {@link #getChildNodeCount(long)} method
 * is similarly implemented based on {@link #getChildNodeEntries()}.
 * Subclasses should normally override these method with a more efficient
 * alternatives.
 */
public abstract class AbstractNodeState implements NodeState {
    private static final int CHILDREN_CAP = getInteger("oak.children.cap", 100);

    public static boolean isValidName(String name) {
        return name != null && !name.isEmpty() && name.indexOf('/') == -1;
    }

    public static void checkValidName(String name)
            throws IllegalArgumentException {
        if (!isValidName(name)) {
            throw new IllegalArgumentException("Invalid name: " + name);
        }
    }

    public static boolean getBoolean(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    public static long getLong(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && property.getType() == LONG) {
            return property.getValue(LONG);
        } else {
            return 0;
        }
    }

    public static String getString(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && property.getType() == STRING) {
            return property.getValue(STRING);
        } else {
            return null;
        }
    }

    public static Iterable<String> getStrings(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && property.getType() == STRINGS) {
            return property.getValue(STRINGS);
        } else {
            return emptyList();
        }
    }

    public static String getName(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    public static Iterable<String> getNames(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

    /**
     * Generic default comparison algorithm that simply walks through the
     * property and child node lists of the given base state and compares
     * the entries one by one with corresponding ones (if any) in this state.
     */
    public static boolean compareAgainstBaseState(
            NodeState state, NodeState base, NodeStateDiff diff) {
        if (!comparePropertiesAgainstBaseState(state, base, diff)) {
            return false;
        }

        Set<String> baseChildNodes = new HashSet<String>();
        for (ChildNodeEntry beforeCNE : base.getChildNodeEntries()) {
            String name = beforeCNE.getName();
            NodeState beforeChild = beforeCNE.getNodeState();
            NodeState afterChild = state.getChildNode(name);
            if (!afterChild.exists()) {
                if (!diff.childNodeDeleted(name, beforeChild)) {
                    return false;
                }
            } else {
                baseChildNodes.add(name);
                if (afterChild != beforeChild) { // TODO: fastEquals?
                    if (!diff.childNodeChanged(name, beforeChild, afterChild)) {
                        return false;
                    }
                }
            }
        }

        for (ChildNodeEntry afterChild : state.getChildNodeEntries()) {
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
     * Compares the properties of {@code base} state with {@code this}
     * state.
     *
     * @param state the head node state.
     * @param base the base node state.
     * @param diff the node state diff.
     * @return {@code true} to continue the comparison, {@code false} to stop
     */
    public static boolean comparePropertiesAgainstBaseState(
            NodeState state, NodeState base, NodeStateDiff diff) {
        Set<String> baseProperties = new HashSet<String>();
        for (PropertyState beforeProperty : base.getProperties()) {
            String name = beforeProperty.getName();
            PropertyState afterProperty = state.getProperty(name);
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

        for (PropertyState afterProperty : state.getProperties()) {
            if (!baseProperties.contains(afterProperty.getName())) {
                if (!diff.propertyAdded(afterProperty)) {
                    return false;
                }
            }
        }

        return true;
    }

    public static String toString(NodeState state) {
        if (!state.exists()) {
            return "{N/A}";
        }
        StringBuilder builder = new StringBuilder("{");
        String separator = " ";
        for (PropertyState property : state.getProperties()) {
            builder.append(separator);
            separator = ", ";
            builder.append(property);
        }
        int count = CHILDREN_CAP;
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (count-- == 0) {
                builder.append("...");
                break;
            }
            builder.append(separator);
            separator = ", ";
            builder.append(entry);
        }
        builder.append(" }");
        return builder.toString();
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return getProperty(name) != null;
    }

    @Override
    public boolean getBoolean(@Nonnull String name) {
        return getBoolean(this, name);
    }

    @Override
    public long getLong(String name) {
        return getLong(this, name);
    }

    @Override
    public String getString(String name) {
        return getString(this, name);
    }

    @Nonnull
    @Override
    public Iterable<String> getStrings(@Nonnull String name) {
        return getStrings(this, name);
    }

    @Override @CheckForNull
    public String getName(@Nonnull String name) {
        return getName(this, name);
    }

    @Override @Nonnull
    public Iterable<String> getNames(@Nonnull String name) {
        return getNames(this, name);
    }

    @Override
    public PropertyState getProperty(@Nonnull String name) {
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
    public long getChildNodeCount(long max) {
        long n = 0;
        Iterator<?> iterator = getChildNodeEntries().iterator();
        while (iterator.hasNext()) {
            iterator.next();
            n++;
            if (n >= max) {
                return Long.MAX_VALUE;
            }
        }
        return n;
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
        return compareAgainstBaseState(this, base, diff);
    }

    /**
     * Returns a string representation of this node state.
     *
     * @return string representation
     */
    public String toString() {
        return toString(this);
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
        } else if (that instanceof NodeState) {
            return equals(this, (NodeState) that);
        } else {
            return false;
        }
    }

    public static boolean equals(NodeState a, NodeState b) {
        if (a.exists() != b.exists()
                || a.getPropertyCount() != b.getPropertyCount()) {
            return false; // shortcut
        }

        // if one of the objects has few entries,
        // then compare the number of entries with the other one
        long max = 20;
        long c1 = a.getChildNodeCount(max);
        long c2 = b.getChildNodeCount(max);
        if (c1 <= max || c2 <= max) {
            // one has less than max entries
            if (c1 != c2) {
                return false;
            }
        } else if (c1 != Long.MAX_VALUE && c2 != Long.MAX_VALUE) {
            // we know the exact number for both
            if (c1 != c2) {
                return false;
            }
        }

        for (PropertyState property : a.getProperties()) {
            if (!property.equals(b.getProperty(property.getName()))) {
                return false;
            }
        }

        // TODO inefficient unless there are very few child nodes

        // compare the exact child node count
        // (before, we only compared up to 20 entries)
        c1 = a.getChildNodeCount(Long.MAX_VALUE);
        c2 = b.getChildNodeCount(Long.MAX_VALUE);
        if (c1 != c2) {
            return false;
        }

        // compare all child nodes recursively (this is potentially very slow,
        // as it recursively calls equals)
        for (ChildNodeEntry entry : a.getChildNodeEntries()) {
            if (!entry.getNodeState().equals(b.getChildNode(entry.getName()))) {
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
