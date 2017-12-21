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

import java.util.List;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.blob.serializer.BlobIdSerializer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkState;

public class NodeStateEntryWriter {
    private static final String OAK_CHILD_ORDER = ":childOrder";
    private static final String DELIMITER = "|";
    private final JsopBuilder jw = new JsopBuilder();
    private final JsonSerializer serializer;
    private final Joiner pathJoiner = Joiner.on('/');

    //TODO Possible optimizations
    //1. Compression
    //2. Dictionary for properties

    public NodeStateEntryWriter(BlobStore blobStore) {
        this.serializer = new JsonSerializer(jw, new BlobIdSerializer(blobStore));
    }

    public String toString(NodeStateEntry e) {
        return toString(e.getPath(), asJson(e.getNodeState()));
    }

    public String toString(String path, String nodeStateAsJson) {
        StringBuilder sb = new StringBuilder(nodeStateAsJson.length() + path.length() + 1);
        sb.append(path)
                .append(DELIMITER)
                .append(nodeStateAsJson);
        return sb.toString();
    }

    public String toString(List<String> pathElements, String nodeStateAsJson) {
        int pathStringSize = pathElements.stream().mapToInt(String::length).sum();
        StringBuilder sb = new StringBuilder(nodeStateAsJson.length() + pathStringSize + pathElements.size() + 1);
        sb.append('/');
        pathJoiner.appendTo(sb, pathElements);
        sb.append(DELIMITER).append(nodeStateAsJson);
        return sb.toString();
    }

    public String asJson(NodeState nodeState) {
        jw.resetWriter();
        jw.object();
        for (PropertyState ps : nodeState.getProperties()) {
            String name = ps.getName();
            if (include(name)) {
                jw.key(name);
                serializer.serialize(ps);
            }
        }
        jw.endObject();
        return jw.toString();
    }

    private boolean include(String propertyName) {
        return !OAK_CHILD_ORDER.equals(propertyName);
    }

    //~-----------------------------------< Utilities to parse >

    public static String getPath(String entryLine){
        return entryLine.substring(0, getDelimiterPosition(entryLine));
    }

    public static String[] getParts(String line) {
        int pos = getDelimiterPosition(line);
        return new String[] {line.substring(0, pos), line.substring(pos + 1)};
    }

    private static int getDelimiterPosition(String entryLine) {
        int indexOfPipe = entryLine.indexOf(NodeStateEntryWriter.DELIMITER);
        checkState(indexOfPipe > 0, "Invalid path entry [%s]", entryLine);
        return indexOfPipe;
    }
}
