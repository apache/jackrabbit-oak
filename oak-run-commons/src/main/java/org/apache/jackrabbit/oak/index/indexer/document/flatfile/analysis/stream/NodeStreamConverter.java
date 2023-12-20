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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import net.jpountz.lz4.LZ4FrameOutputStream;

/**
 * Allows to convert a flat-file store to a node stream.
 */
public class NodeStreamConverter {

    public static void main(String... args) throws IOException {
        String sourceFileName = args[0];
        String targetFileName = args[1];
        convert(sourceFileName, targetFileName);
    }

    public static void convert(String sourceFileName, String targetFileName) throws IOException {
        try (NodeLineReader in = NodeLineReader.open(sourceFileName)) {
            try (OutputStream fileOut = new BufferedOutputStream(new FileOutputStream(targetFileName))) {
                try (OutputStream out = new LZ4FrameOutputStream(fileOut)) {
                    while (true) {
                        NodeData node = in.readNode();
                        if (node == null) {
                            break;
                        }
                        writeNode(out, node);
                    }
                }
            }
        }
    }

    private static void writeNode(OutputStream out, NodeData node) throws IOException {
        writeVarInt(out, node.getPathElements().size());
        for(String s : node.getPathElements()) {
            writeString(out, s);
        }
        writeVarInt(out, node.getProperties().size());
        for (NodeProperty p : node.getProperties()) {
            writeString(out, p.getName());
            out.write(p.getType().getOrdinal());
            if (p.isMultiple()) {
                out.write(1);
                writeVarInt(out, p.getValues().length);
                for (String s : p.getValues()) {
                    writeString(out, s);
                }
            } else {
                out.write(0);
                writeString(out, p.getValues()[0]);
            }
        }
    }

    private static void writeString(OutputStream out, String s) throws IOException {
        if (s == null) {
            writeVarInt(out, -1);
        } else {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            writeVarInt(out, utf8.length);
            out.write(utf8);
        }
    }

    /**
     * Write a variable size int.
     *
     * @param out the output stream
     * @param x the value
     * @throws IOException if some data could not be written
     */
    public static void writeVarInt(OutputStream out, int x) throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((byte) (x | 0x80));
            x >>>= 7;
        }
        out.write((byte) x);
    }

}
