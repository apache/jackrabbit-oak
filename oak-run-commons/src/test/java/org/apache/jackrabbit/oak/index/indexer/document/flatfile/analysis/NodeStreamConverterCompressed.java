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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import net.jpountz.lz4.LZ4FrameOutputStream;

public class NodeStreamConverterCompressed {
    
    private static final int WINDOW_SIZE = 1024;
    private static final int CACHE_SIZE = 8 * 1024;
    private static final int MAX_LENGTH = 1024;
    
    private final OutputStream out;
    private final long[] cacheId = new long[CACHE_SIZE];
    private final String[] cache = new String[CACHE_SIZE];
    private long currentId;
    
    NodeStreamConverterCompressed(OutputStream out) {
        this.out = out;
    }

    public static void main(String... args) throws IOException {
        String sourceFileName = args[0];
        String targetFileName = args[1];
        NodeLineReader in = NodeLineReader.open(sourceFileName);
        OutputStream out = new LZ4FrameOutputStream(
                new FileOutputStream(targetFileName));
        NodeStreamConverterCompressed writer = new NodeStreamConverterCompressed(out);
        int count = 0;
        while (true) {
            NodeData node = in.readNode();
            if (node == null) {
                break;
            }
            writer.writeNode(node);
            if (++count % 1000000 == 0) {
                System.out.println(count + " lines converted");
            }
        }
        out.close();
    }

    private void writeNode(NodeData node) throws IOException {
        writeVarInt(out, node.getPathElements().size());
        for(String s : node.getPathElements()) {
            writeString(s);
        }
        writeVarInt(out, node.getProperties().size());
        for (Property p : node.getProperties()) {
            writeString(p.getName());
            out.write(p.getType().getOrdinal());
            if (p.isMultiple()) {
                out.write(1);
                writeVarInt(out, p.getValues().length);
                for (String s : p.getValues()) {
                    writeString(s);
                }
            } else {
                out.write(0);
                writeString(p.getValues()[0]);
            }
        }
    }

    private void writeString(String s) throws IOException {
        if (s == null) {
            writeVarInt(out, 0);
            return;
        }
        int len = s.length();
        if (len < MAX_LENGTH) {
            if (len == 0) {
                writeVarInt(out, 1);
                return;
            }
            int index = s.hashCode() & (CACHE_SIZE - 1);
            String old = cache[index];
            if (old != null && old.equals(s)) {
                long offset = currentId - cacheId[index];
                if (offset < WINDOW_SIZE) {
                    cacheId[index] = currentId++;
                    writeVarInt(out, (int) ((offset << 1) | 1));
                    return;
                }
            }
            cacheId[index] = currentId++;
            cache[index] = s;
        }
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        writeVarInt(out, utf8.length << 1);
        out.write(utf8);
    }

    /**
     * Write a variable size int.
     *
     * @param out the output stream
     * @param x the value
     * @throws IOException if some data could not be written
     */
    private static void writeVarInt(OutputStream out, int x) throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((byte) (x | 0x80));
            x >>>= 7;
        }
        out.write((byte) x);
    }

}
