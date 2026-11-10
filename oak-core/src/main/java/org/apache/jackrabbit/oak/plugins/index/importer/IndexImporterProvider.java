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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * This class is responsible for importing the index
 * data from the file system into the repository.
 */
public interface IndexImporterProvider {

    /**
     * Import the index data from given directory into the
     * NodeBuilder created for the index at given path
     */
    void importIndex(NodeState root, NodeBuilder defn, File indexDir) throws IOException, CommitFailedException;

    /**
     * Release backend resources for a new index abandoned by a failed import.
     * The importer calls this only after removing the definition it installed.
     * Implementations must verify that the generated resources are no longer
     * referenced and delete only artifacts belonging to this definition.
     * Repository-contained index data is removed with the definition; shared
     * blobs must not be deleted directly.
     *
     * @param nodeStore repository after definition removal
     * @param indexPath path of the removed definition
     * @param definition committed definition of the abandoned import
     */
    default void cleanupFailedImport(NodeStore nodeStore, String indexPath, NodeState definition)
            throws IOException {
    }

    /**
     * Index type for this implementation
     */
    String getType();
}
