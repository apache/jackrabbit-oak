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

package org.apache.jackrabbit.oak.exporter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import com.google.common.io.Files;
import com.google.gson.stream.JsonWriter;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.blob.serializer.FSBlobSerializer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class NodeStateSerializer {
    public enum Format {JSON, TXT}

    private final NodeState nodeState;
    private String blobDirName = "blobs";
    private String jsonFileName = "nodestates.json";
    private String txtFileName = "nodestates.txt";
    private int depth = Integer.MAX_VALUE;
    private int maxChildNodes = Integer.MAX_VALUE;
    private String filter = "{}";
    private File filterFile;
    private String path = "/";
    private Format format = Format.JSON;
    private boolean serializeBlobContent;
    private boolean prettyPrint = true;
    private FSBlobSerializer blobSerializer;

    public NodeStateSerializer(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    public String serialize() throws IOException {
        StringWriter sw = new StringWriter();
        serialize(sw, createBlobSerializer());
        closeSerializer();
        return sw.toString();
    }

    public void serialize(File dir) throws IOException {
        if (dir.exists()) {
            checkArgument(dir.isDirectory(), "Input file must be directory [%s]", dir.getAbsolutePath());
        } else {
            checkState(dir.mkdirs(), "Cannot create directory [%s]", dir.getAbsolutePath());
        }
        File file = new File(dir, getFileName());
        try (Writer writer = Files.newWriter(file, UTF_8)){
            serialize(writer, createBlobSerializer(dir));
        }
        closeSerializer();
    }

    private void serialize(Writer writer, BlobSerializer blobSerializer) throws IOException {
        JsopWriter jsopWriter = null;
        if (format == Format.JSON) {
            JsonWriter jw = new JsonWriter(writer);
            if (prettyPrint) {
                jw.setIndent(" ");
            }
            jsopWriter = new JsopStreamWriter(jw);
        } else if (format == Format.TXT) {
            jsopWriter = new CNDStreamWriter(new PrintWriter(writer));
        }

        serialize(jsopWriter, blobSerializer);

        if (jsopWriter instanceof Closeable) {
            ((Closeable) jsopWriter).close();
        }
    }

    private void serialize(JsopWriter writer, BlobSerializer blobSerializer) throws IOException {
        JsonSerializer serializer = new JsonSerializer(writer, depth, 0, maxChildNodes, getFilter(), blobSerializer);
        NodeState state = NodeStateUtils.getNode(nodeState, path);
        serializer.serialize(state);
    }

    private BlobSerializer createBlobSerializer(File dir) {
        if (!serializeBlobContent) {
            return new BlobSerializer();
        }
        File blobs = new File(dir, blobDirName);
        blobSerializer = new FSBlobSerializer(blobs);
        return blobSerializer;
    }

    private void closeSerializer() throws IOException {
        if (blobSerializer != null) {
            blobSerializer.close();
        }
    }

    private BlobSerializer createBlobSerializer() {
        return serializeBlobContent ? new Base64BlobSerializer() : new BlobSerializer();
    }

    private String getFilter() throws IOException {
        return filterFile != null ? Files.toString(filterFile, UTF_8) : filter;
    }

    public String getFileName() {
        return format == Format.JSON ? jsonFileName : txtFileName;
    }

    public String getBlobDirName() {
        return blobDirName;
    }

    public void setBlobDirName(String blobDirName) {
        this.blobDirName = blobDirName;
    }

    public void setJsonFileName(String jsonFileName) {
        this.jsonFileName = jsonFileName;
    }

    public void setTxtFileName(String txtFileName) {
        this.txtFileName = txtFileName;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public void setMaxChildNodes(int maxChildNodes) {
        this.maxChildNodes = maxChildNodes;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setFilterFile(File filterFile) {
        this.filterFile = filterFile;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public void setSerializeBlobContent(boolean serializeBlobContent) {
        this.serializeBlobContent = serializeBlobContent;
    }

    public void setPrettyPrint(boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
    }
}
