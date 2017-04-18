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


import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;

/**
 * A {@code ChildNodeEntry} instance represents the child node states of a
 * {@link NodeState}.
 * <h2>Equality and hash codes</h2>
 * <p>
 * Two child node entries are considered equal if and only if their names
 * and referenced node states match. The {@link Object#equals(Object)}
 * method needs to be implemented so that it complies with this definition.
 * And while child node entries are not meant for use as hash keys, the
 * {@link Object#hashCode()} method should still be implemented according
 * to this equality contract.
 */
public interface ChildNodeEntry {

    /**
     * The name of the child node state wrt. to its parent state.
     * @return  name of the child node
     */
    @Nonnull
    String getName();

    /**
     * The child node state
     * @return child node state
     */
    @Nonnull
    NodeState getNodeState();

    /**
     * Mapping from a ChildNodeEntry instance to its name.
     */
    Function<ChildNodeEntry, String> GET_NAME =
            new Function<ChildNodeEntry, String>() {
                @Override @Nullable
                public String apply(@Nullable ChildNodeEntry input) {
                    if (input != null) {
                        return input.getName();
                    } else {
                        return null;
                    }
                }
            };

}
