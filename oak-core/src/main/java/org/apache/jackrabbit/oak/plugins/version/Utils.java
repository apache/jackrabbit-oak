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
package org.apache.jackrabbit.oak.plugins.version;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code Utils} provide some utility methods.
 */
final class Utils {
    private Utils() {
    }

    /**
     * Returns the jcr:uuid value of given {@code node}.
     *
     * @param node a referenceable node.
     * @return the value of the jcr:uuid property.
     * @throws IllegalArgumentException if the node is not referenceable.
     */
    @Nonnull
    static String uuidFromNode(@Nonnull NodeBuilder node)
            throws IllegalArgumentException {
        return uuidFromNode(node.getNodeState());
    }

    @Nonnull
    static String uuidFromNode(@Nonnull NodeState node) {
        PropertyState p = checkNotNull(node).getProperty(JCR_UUID);
        if (p == null) {
            throw new IllegalArgumentException("Not referenceable");
        }
        return p.getValue(Type.STRING);
    }

    /**
     * Returns the {@code jcr:primaryType} value of the given
     * {@code node}.
     *
     * @param node a node.
     * @return the {@code jcr:primaryType} value.
     * @throws IllegalStateException if the node does not have a {@code jcr:primaryType}
     *                               property.
     */
    @Nonnull
    static String primaryTypeOf(@Nonnull NodeBuilder node)
            throws IllegalStateException {
        String primaryType = checkNotNull(node).getName(JCR_PRIMARYTYPE);
        if (primaryType == null) {
            throw new IllegalStateException("Node does not have a jcr:primaryType");
        }
        return primaryType;
    }

    static <T> T throwProtected(String path) throws CommitFailedException {
        throw new CommitFailedException(CONSTRAINT, 100,
                "Item is protected: " + path);
    }
}
