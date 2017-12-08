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

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.blob.serializer.BlobIdSerializer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;

public class NodeStateEntryWriter implements Closeable{
    private static final String OAK_CHILD_ORDER = ":childOrder";
    private static final String DELIMITER = "|";
    private final Writer writer;
    private final JsopBuilder jw = new JsopBuilder();
    private final JsonSerializer serializer;

    //TODO Possible optimizations
    //1. Compression
    //2. Dictionary for properties

    public NodeStateEntryWriter(BlobStore blobStore, Writer writer) {
        this.writer = writer;
        this.serializer = new JsonSerializer(jw, new BlobIdSerializer(blobStore));
    }

    public void write(NodeStateEntry e) throws IOException {
        String text = asText(e.getNodeState());
        writer.append(e.getPath())
                .append(DELIMITER)
                .append(text)
                .append(LINE_SEPARATOR.value());
    }

    @Override
    public void close() throws IOException {
        writer.flush();
    }

    private String asText(NodeState nodeState) {
        return asJson(nodeState);
    }

    private String asJson(NodeState nodeState) {
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
