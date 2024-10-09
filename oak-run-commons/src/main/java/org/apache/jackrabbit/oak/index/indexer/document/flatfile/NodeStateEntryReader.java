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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.json.BlobDeserializer;
import org.apache.jackrabbit.oak.json.JsonDeserializer;
import org.apache.jackrabbit.oak.plugins.blob.serializer.BlobIdSerializer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.commons.StringUtils.estimateMemoryUsage;

public class NodeStateEntryReader {
    private final JsonDeserializer des;

    public NodeStateEntryReader(BlobStore blobStore) {
        BlobDeserializer blobDeserializer = new BlobIdSerializer(blobStore);
        this.des = new JsonDeserializer(blobDeserializer);
    }

    public NodeStateEntry read(String ffsLine) {
        long memUsage = estimateMemoryUsage(ffsLine);
        var idx = ffsLine.indexOf('|');
        var path = ffsLine.substring(0, idx);
        NodeState nodeState = des.deserialize(ffsLine, idx + 1);
        return new NodeStateEntry(nodeState, path, memUsage, 0, "");
    }
}
