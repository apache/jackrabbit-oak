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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;

/**
 * A builder for Lucene directories.
 */
public interface DirectoryFactory {

    /**
     * Open a new directory.
     * 
     * Internally, it read the data from the index definition. It writes to the
     * builder, for example when closing the directory.
     * 
     * @param definition the index definition
     * @param builder the builder pointing to the index definition (see above
     *            for usage)
     * @param dirName the name of the directory (in the file system)
     * @param reindex whether reindex is needed
     * @return the Lucene directory
     */
    Directory newInstance(LuceneIndexDefinition definition, NodeBuilder builder, String dirName,
                          boolean reindex) throws IOException;

    /**
     * Determines if the Directory is having a remote storage
     * or local storage
     */
    boolean remoteDirectory();

}
