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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeStreamReader.readVarInt;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;

import net.jpountz.lz4.LZ4FrameInputStream;

/**
 * A node stream reader with compression for repeated strings.
 */
public class NodeStreamReaderCompressed implements NodeDataReader {

    private static final int MAX_LENGTH = 1024;
    private static final int WINDOW_SIZE = 1024;

    private final CountingInputStream countIn;
    private final InputStream in;
    private final long fileSize;
    private final String[] lastStrings = new String[WINDOW_SIZE];

    private long currentId;
    private byte[] buffer = new byte[1024 * 1024];

    private NodeStreamReaderCompressed(CountingInputStream countIn, InputStream in, long fileSize) {
        this.countIn = countIn;
        this.in = in;
        this.fileSize = fileSize;
    }

    public int getProgressPercent() {
        return (int) (100 * countIn.getByteCount() / Math.max(1, fileSize));
    }

    public static NodeStreamReaderCompressed open(String fileName) throws IOException {
        long fileSize = new File(fileName).length();
        InputStream fileIn = new FileInputStream(fileName);
        CountingInputStream countIn = new CountingInputStream(fileIn);
        try {
            InputStream in;
            if (fileName.endsWith(".lz4")) {
                in = new LZ4FrameInputStream(countIn); //NOSONAR
            } else {
                in = countIn;
            }
            return new NodeStreamReaderCompressed(countIn, in, fileSize);
        } catch (IOException e) {
            countIn.close();
            throw e;
        }
    }

    public NodeData readNode() throws IOException {
        int size = readVarInt(in);
        if (size < 0) {
            close();
            return null;
        }
        ArrayList<String> pathElements = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            pathElements.add(readString(in));
        }
        int propertyCount = readVarInt(in);
        ArrayList<NodeProperty> properties = new ArrayList<>(propertyCount);
        for (int i = 0; i < propertyCount; i++) {
            NodeProperty p;
            String name = readString(in);
            ValueType type = ValueType.byOrdinal(in.read());
            if (in.read() == 1) {
                int count = readVarInt(in);
                String[] values = new String[count];
                for (int j = 0; j < count; j++) {
                    values[j] = readString(in);
                }
                p = new NodeProperty(name, type, values, true);
            } else {
                String value = readString(in);
                p = new NodeProperty(name, type, value);
            }
            properties.add(p);
        }
        return new NodeData(pathElements, properties);
    }

    private String readString(InputStream in) throws IOException {
        int len = readVarInt(in);
        if (len < 2) {
            if (len == 0) {
                return null;
            } else if (len == 1) {
                return "";
            }
        }
        if ((len & 1) == 1) {
            int offset = len >>> 1;
            String s = lastStrings[(int) (currentId - offset) & (WINDOW_SIZE - 1)];
            lastStrings[(int) currentId & (WINDOW_SIZE - 1)] = s;
            currentId++;
            return s;
        }
        len = len >>> 1;
        byte[] buff = buffer;
        if (len > buff.length) {
            buff = buffer = new byte[len];
        }
        int read = in.readNBytes(buff, 0, len);
        if (read != len) {
            throw new EOFException();
        }
        String s = new String(buff, 0, len, StandardCharsets.UTF_8);
        if (s.length() < MAX_LENGTH) {
            lastStrings[(int) currentId & (WINDOW_SIZE - 1)] = s;
            currentId++;
        }
        return s;
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

}
