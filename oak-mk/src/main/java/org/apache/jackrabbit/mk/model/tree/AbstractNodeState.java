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
package org.apache.jackrabbit.mk.model.tree;

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
 * {@link #getChildNodeEntries(long, int)}. Subclasses should normally
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
    @SuppressWarnings("unused")
    public long getPropertyCount() {
        long count = 0;
        for (PropertyState property : getProperties()) {
            count++;
        }
        return count;
    }

    @Override
    public NodeState getChildNode(String name) {
        for (ChildNode entry : getChildNodeEntries(0, -1)) {
            if (name.equals(entry.getName())) {
                return entry.getNode();
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unused")
    public long getChildNodeCount() {
        long count = 0;
        for (ChildNode entry : getChildNodeEntries(0, -1)) {
            count++;
        }
        return count;
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

        long propertyCount = 0;
        for (PropertyState property : getProperties()) {
            if (!property.equals(other.getProperty(property.getName()))) {
                return false;
            }
            propertyCount++;
        }
        if (propertyCount != other.getPropertyCount()) {
            return false;
        }

        long childNodeCount = 0;
        for (ChildNode entry : getChildNodeEntries(0, -1)) {
            if (!entry.getNode().equals(other.getChildNode(entry.getName()))) {
                return false;
            }
            childNodeCount++;
        }
        return childNodeCount == other.getChildNodeCount();
    }

    /**
     * Returns the hash code. This method is relatively expensive, and the
     * returned value is not very distinct, as {@link NodeState} instances are
     * not intended for use as hash keys.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        int hash = (int) getChildNodeCount();
        for (PropertyState p : getProperties()) {
            hash ^= p.hashCode();
        }
        return hash;
    }

}
