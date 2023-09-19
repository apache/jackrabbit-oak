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

import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;

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
        List<Integer> positions = getDelimiterPositions(line);
        checkState(positions.size() >= 3, "Invalid path entry [%s]", line);
        // there are 4 parts in incrementalFFS and default delimiter is |
        // path|nodeData|checkpoint|operand
        // Node's data can itself have many | so we split based on first and last 2 |
        String[] parts = new String[4];
        parts[0] = line.substring(0, positions.get(0));
        parts[1] = line.substring(positions.get(0) + 1, positions.get(positions.size() - 2));
        parts[2] = line.substring(positions.get(positions.size() - 2) + 1, positions.get(positions.size() - 1));
        parts[3] = line.substring(positions.get(positions.size() - 1) + 1);
        return parts;
    }

    private static List<Integer> getDelimiterPositions(String entryLine) {
        List<Integer> indexPositions = new ArrayList<>(3);

        int index = 0;
        while ((index = entryLine.indexOf(NodeStateEntryWriter.DELIMITER, index)) != -1) {
            indexPositions.add(index);
            index++;
        }
        return indexPositions;
    }

}
