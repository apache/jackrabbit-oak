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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Factory for creating blobs from input streams.
 * Adapted from oak-lucene for Lucene 9.
 */
@FunctionalInterface
public interface BlobFactory {

    /**
     * Create a blob from an input stream.
     *
     * @param in the input stream
     * @return the created blob
     * @throws IOException if blob creation fails
     */
    Blob createBlob(InputStream in) throws IOException;

    /**
     * Get a BlobFactory that uses NodeBuilder.createBlob().
     *
     * @param builder the node builder
     * @return a blob factory
     */
    static BlobFactory getNodeBuilderBlobFactory(final NodeBuilder builder) {
        return builder::createBlob;
    }
}
