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

public class NodeStateEntryReader {
    private final BlobDeserializer blobDeserializer;

    public NodeStateEntryReader(BlobStore blobStore){
        this.blobDeserializer = new BlobIdSerializer(blobStore);
    }

    public NodeStateEntry read(String line){
        String[] parts = NodeStateEntryWriter.getParts(line);
        return new NodeStateEntry(parseState(parts[1]), parts[0]);
    }

    private NodeState parseState(String part) {
        JsonDeserializer des = new JsonDeserializer(blobDeserializer);
        return des.deserialize(part);
    }
}
