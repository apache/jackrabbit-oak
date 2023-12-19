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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NodeStreamTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void test() throws IOException {
        File f = temporaryFolder.getRoot();
        File flatFile = new File(f, "flatFile.txt");
        BufferedWriter w = new BufferedWriter(new FileWriter(flatFile));
        for (int i = 0; i < 10; i++) {
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < 16; j++) {
                if (j > 0) {
                    buff.append(",");
                }
                String value = Integer.toHexString(j).repeat(1 << j);
                buff.append("\"x" + j + "\":\"" + value + "\"");
            }
            buff.append(",\"a\":true");
            buff.append(",\"b\":false");
            buff.append(",\"c\":\":blobId:0x12\"");
            buff.append(",\"d\":\"str:1\"");
            buff.append(",\"e\":\"nam:2\"");
            buff.append(",\"f\":\"ref:3\"");
            buff.append(",\"g\":\"dat:4\"");
            buff.append(",\"h\":\"dec:5\"");
            buff.append(",\"i\":\"dou:6\"");
            buff.append(",\"j\":\"wea:7\"");
            buff.append(",\"k\":\"uri:8\"");
            buff.append(",\"l\":\"pat:9\"");
            buff.append(",\"m\":\"[0]:Name\"");
            w.write("/n" + i + "|{" + buff.toString() +
                    ",\"n\":null,\"x\":[\"1\"]}\n");
        }
        w.close();

        File streamFile = new File(f, "streamFile.lz4");
        File compressedStreamFile = new File(f, "compressedStreamFile.lz4");

        NodeStreamConverter.convert(flatFile.getAbsolutePath(), streamFile.getAbsolutePath());
        NodeStreamConverterCompressed.convert(flatFile.getAbsolutePath(), compressedStreamFile.getAbsolutePath());

        NodeDataReader flatReader = NodeLineReader.open(flatFile.getAbsolutePath());
        long fileSize1 = flatReader.getFileSize();
        NodeDataReader nodeStream = NodeStreamReader.open(streamFile.getAbsolutePath());
        long fileSize2 = nodeStream.getFileSize();
        NodeDataReader compressedStream = NodeStreamReaderCompressed.open(compressedStreamFile.getAbsolutePath());
        long fileSize3 = compressedStream.getFileSize();
        assertTrue(fileSize3 < fileSize2);
        assertTrue(fileSize2 < fileSize1);
        while (true) {
            NodeData n1 = flatReader.readNode();
            NodeData n2 = nodeStream.readNode();
            NodeData n3 = compressedStream.readNode();
            if (n1 == null) {
                assertNull(n2);
                assertNull(n3);
                break;
            }
            assertEquals(n1.toString(), n2.toString());
            assertEquals(n1.toString(), n3.toString());
        }
    }
}
