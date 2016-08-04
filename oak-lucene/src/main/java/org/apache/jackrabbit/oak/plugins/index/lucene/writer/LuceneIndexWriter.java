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

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import java.io.IOException;

import org.apache.lucene.index.IndexableField;

public interface LuceneIndexWriter {

    /**
     * Updates the Lucene document having given path
     *
     * @param path path of the NodeState which the Document represents
     * @param doc updated document
     */
    void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException;

    /**
     * Deletes Lucene Documents which are same or child of given path
     *
     * @param path path whose children need to be deleted
     */
    void deleteDocuments(String path) throws IOException;

    /**
     * Closes the underlying writer.
     *
     * @param timestamp timestamp to be used for recording at status in NodeBuilder
     * @return true if index was updated or any write happened.
     */
    boolean close(long timestamp) throws IOException;
}
