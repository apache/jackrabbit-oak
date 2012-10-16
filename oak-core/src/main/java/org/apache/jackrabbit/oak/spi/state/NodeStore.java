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

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;

/**
 * Storage abstraction for trees. At any given point in time the stored
 * tree is rooted at a single immutable node state.
 * <p>
 * This is a low-level interface that doesn't cover functionality like
 * merging concurrent changes or rejecting new tree states based on some
 * higher-level consistency constraints.
 */
public interface NodeStore {

    /**
     * Returns the latest state of the tree.
     *
     * @return root node state
     */
    @Nonnull
    NodeState getRoot();

    /**
     * Creates a new branch of the tree to which transient changes can be applied.
     *
     * @return branch
     */
    @Nonnull
    NodeStoreBranch branch();

    /**
     * Create a {@link Blob} from the given input stream. The input stream
     * is closed after this method returns.
     * @param inputStream  The input stream for the {@code Blob}
     * @return  The {@code Blob} representing {@code inputStream}
     * @throws IOException  If an error occurs while reading from the stream
     */
    Blob createBlob(InputStream inputStream) throws IOException;
}
