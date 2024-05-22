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

package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public class IncrementalFlatFileStoreNodeStateEntryWriter extends NodeStateEntryWriter {

    /**
     * Todo - In this implementation we are only changing get parts logic.
     * May be we can refactor it to make this more generic.
     */
    public IncrementalFlatFileStoreNodeStateEntryWriter(BlobStore blobStore) {
        this(blobStore, false);
    }

    public IncrementalFlatFileStoreNodeStateEntryWriter(BlobStore blobStore, boolean includeChildOrder) {
        super(blobStore, includeChildOrder);
    }

    public static String[] getParts(String line) {
        // there are 4 parts in incrementalFFS and default delimiter is |
        // path|nodeData|checkpoint|operand
        // Node's data can itself have many | so we split based on first and last 2 |
        int startIndex = -1;
        int lastIndex = -1;

        StringBuilder stringBuilder = new StringBuilder(line);

        startIndex = stringBuilder.indexOf(NodeStateEntryWriter.DELIMITER);
        String path = stringBuilder.substring(0, startIndex);
        stringBuilder.delete(0, startIndex + 1);

        lastIndex = stringBuilder.lastIndexOf(NodeStateEntryWriter.DELIMITER);
        String operand = stringBuilder.substring(lastIndex + 1);
        stringBuilder.delete(lastIndex, stringBuilder.length());

        lastIndex = stringBuilder.lastIndexOf(NodeStateEntryWriter.DELIMITER);
        String checkpoint = stringBuilder.substring(lastIndex + 1);
        stringBuilder.delete(lastIndex, stringBuilder.length());

        String nodeData = stringBuilder.toString();

        String[] parts = new String[4];
        parts[0] = path;
        parts[1] = nodeData;
        parts[2] = checkpoint;
        parts[3] = operand;
        return parts;
    }

}
