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

package org.apache.jackrabbit.oak.plugins.index.search.spi.editor;

import java.io.IOException;

/**
 * A {@link FulltextIndexWriter} is responsible for writing / deleting documents of type D to the
 * index implementation underlying persistence layer.
 */
public interface FulltextIndexWriter<D> {

    /**
     * Updates the document having given path
     *
     * @param path path of the NodeState which the Document represents
     * @param doc  updated document
     */
    void updateDocument(String path, D doc) throws IOException;

    /**
     * Deletes documents which are same or child of given path
     *
     * @param path path whose children need to be deleted
     */
    void deleteDocuments(String path) throws IOException;

    /**
     * Closes the underlying resources.
     *
     * @param timestamp timestamp to be used for recording at status in NodeBuilder
     * @return true if index was updated or any write happened.
     */
    boolean close(long timestamp) throws IOException;
}
