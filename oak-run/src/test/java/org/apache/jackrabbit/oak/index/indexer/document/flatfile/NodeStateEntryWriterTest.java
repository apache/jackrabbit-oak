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

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeStateEntryWriterTest {
    private BlobStore blobStore = new MemoryBlobStore();
    private NodeBuilder builder = EMPTY_NODE.builder();
    private StringWriter sw = new StringWriter();

    @Test
    public void newLines() throws Exception{
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore, sw);

        builder.setProperty("foo", 1);
        builder.setProperty("foo2", Arrays.asList("a", "b"), Type.STRINGS);
        builder.setProperty("foo3", "text with \n new line");
        nw.write(new NodeStateEntry(builder.getNodeState(), "/a"));
        nw.close();

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);
        BufferedReader br = new BufferedReader(new StringReader(sw.toString()));

        String line = br.readLine();
        NodeStateEntry ne = nr.read(line);
        assertEquals("/a", ne.getPath());
        assertEquals("/a", NodeStateEntryWriter.getPath(line));
        assertTrue(EqualsDiff.equals(ne.getNodeState(), builder.getNodeState()));
    }

    @Test
    public void multipleEntries() throws Exception{
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore, sw);

        NodeBuilder b1 = EMPTY_NODE.builder();
        b1.setProperty("foo", "bar");

        NodeBuilder b2 = EMPTY_NODE.builder();
        b2.setProperty("foo2", "bar2");

        NodeStateEntry e1 = new NodeStateEntry(b1.getNodeState(), "/a");
        NodeStateEntry e2 = new NodeStateEntry(b2.getNodeState(), "/a");

        nw.write(e1);
        nw.write(e2);
        nw.close();

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);
        BufferedReader br = new BufferedReader(new StringReader(sw.toString()));

        assertEquals(e1, nr.read(br.readLine()));
        assertEquals(e2, nr.read(br.readLine()));
    }

    @Test
    public void childOrderNotWritten() throws Exception{
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore, sw);

        NodeBuilder b1 = EMPTY_NODE.builder();
        b1.setProperty("foo", "bar");
        b1.setProperty(":childOrder", "bar");
        b1.setProperty(":hidden", "bar");

        NodeStateEntry e1 = new NodeStateEntry(b1.getNodeState(), "/a");

        nw.write(e1);
        nw.close();

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);
        BufferedReader br = new BufferedReader(new StringReader(sw.toString()));

        NodeStateEntry r1 = nr.read(br.readLine());
        assertTrue(r1.getNodeState().hasProperty(":hidden"));
        assertFalse(r1.getNodeState().hasProperty(":childOrder"));
    }

}