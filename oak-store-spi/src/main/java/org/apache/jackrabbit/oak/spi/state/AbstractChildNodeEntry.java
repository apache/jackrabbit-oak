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

/**
 * Abstract base class for {@link ChildNodeEntry} implementations.
 * This base class contains default implementations of the
 * {@link #equals(Object)} and {@link #hashCode()} methods based on
 * the implemented interface.
 */
public abstract class AbstractChildNodeEntry implements ChildNodeEntry {

    /**
     * Returns a string representation of this child node entry.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        String name = getName();
        NodeState state = getNodeState();
        if (state.getChildNodeCount(1) == 0) {
            return name + " : " + state;
        } else {
            return name + " = { ... }";
        }
    }

    /**
     * Checks whether the given object is equal to this one. Two child node
     * entries are considered equal if both their names and referenced node
     * states match. Subclasses may override this method with a more efficient
     * equality check if one is available.
     *
     * @param that target of the comparison
     * @return {@code true} if the objects are equal,
     *         {@code false} otherwise
     */
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof ChildNodeEntry) {
            ChildNodeEntry other = (ChildNodeEntry) that;
            return getName().equals(other.getName())
                    && getNodeState().equals(other.getNodeState());
        } else {
            return false;
        }

    }

    /**
     * Returns a hash code that's compatible with how the
     * {@link #equals(Object)} method is implemented. The current
     * implementation simply returns the hash code of the child node name
     * since {@link ChildNodeEntry} instances are not intended for use as
     * hash keys.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return getName().hashCode();
    }

}
