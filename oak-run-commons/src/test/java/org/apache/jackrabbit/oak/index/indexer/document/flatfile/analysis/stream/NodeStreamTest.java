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
        for(int i=0; i<100; i++) {
            w.write("/n" + i + "|{\"x\":"+i+"}\n");
        }
        w.close();
        
        File streamFile = new File(f, "streamFile.lz4");
        File compressedStreamFile = new File(f, "compressedStreamFile.lz4");
        
        NodeStreamConverter.convert(flatFile.getAbsolutePath(), streamFile.getAbsolutePath());
        NodeStreamConverterCompressed.convert(flatFile.getAbsolutePath(), compressedStreamFile.getAbsolutePath());
        
        NodeDataReader nodeStream = NodeStreamReader.open(streamFile.getAbsolutePath());
        NodeDataReader compressedStream = NodeStreamReaderCompressed.open(compressedStreamFile.getAbsolutePath());
        NodeDataReader flatReader = NodeLineReader.open(flatFile.getAbsolutePath());
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
