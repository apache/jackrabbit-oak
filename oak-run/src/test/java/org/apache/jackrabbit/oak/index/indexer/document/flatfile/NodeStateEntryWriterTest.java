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

import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter.getPath;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeStateEntryWriterTest {
    private BlobStore blobStore = new MemoryBlobStore();
    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void newLines() {
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore);

        builder.setProperty("foo", 1);
        builder.setProperty("foo2", Arrays.asList("a", "b"), Type.STRINGS);
        builder.setProperty("foo3", "text with \n new line");
        String line = nw.toString(new NodeStateEntry(builder.getNodeState(), "/a"));

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);

        NodeStateEntry ne = nr.read(line);
        assertEquals("/a", ne.getPath());
        assertEquals("/a", NodeStateEntryWriter.getPath(line));
        assertTrue(EqualsDiff.equals(ne.getNodeState(), builder.getNodeState()));
    }

    @Test
    public void multipleEntries() {
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore);

        NodeBuilder b1 = EMPTY_NODE.builder();
        b1.setProperty("foo", "bar");

        NodeBuilder b2 = EMPTY_NODE.builder();
        b2.setProperty("foo2", "bar2");

        NodeStateEntry e1 = new NodeStateEntry(b1.getNodeState(), "/a");
        NodeStateEntry e2 = new NodeStateEntry(b2.getNodeState(), "/a");

        String line1 = nw.toString(e1);
        String line2 = nw.toString(e2);

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);

        assertEquals(e1, nr.read(line1));
        assertEquals(e2, nr.read(line2));
    }

    @Test
    public void childOrderNotWritten(){
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore);

        NodeBuilder b1 = EMPTY_NODE.builder();
        b1.setProperty("foo", "bar");
        b1.setProperty(":childOrder", "bar");
        b1.setProperty(":hidden", "bar");

        NodeStateEntry e1 = new NodeStateEntry(b1.getNodeState(), "/a");

        String line = nw.toString(e1);

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);

        NodeStateEntry r1 = nr.read(line);
        assertTrue(r1.getNodeState().hasProperty(":hidden"));
        assertFalse(r1.getNodeState().hasProperty(":childOrder"));
    }

    @Test
    public void pathElements(){
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore);
        NodeBuilder b1 = EMPTY_NODE.builder();
        b1.setProperty("foo", "bar");

        NodeStateEntry e1 = new NodeStateEntry(b1.getNodeState(), "/a/b/c/d");

        String json = nw.asJson(e1.getNodeState());
        List<String> pathElements = copyOf(elements(e1.getPath()));

        String line = nw.toString(pathElements, json);

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);
        NodeStateEntry r1 = nr.read(line);
        assertTrue(r1.getNodeState().hasProperty("foo"));
        assertEquals("/a/b/c/d", r1.getPath());

    }

    @Test
    public void pathElements_root(){
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore);
        NodeBuilder b1 = EMPTY_NODE.builder();
        b1.setProperty("foo", "bar");

        NodeStateEntry e1 = new NodeStateEntry(b1.getNodeState(), "/");

        String json = nw.asJson(e1.getNodeState());
        List<String> pathElements = copyOf(elements(e1.getPath()));

        String line = nw.toString(pathElements, json);

        NodeStateEntryReader nr = new NodeStateEntryReader(blobStore);
        NodeStateEntry r1 = nr.read(line);
        assertTrue(r1.getNodeState().hasProperty("foo"));
        assertEquals("/", r1.getPath());

    }

}