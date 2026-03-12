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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class OakDirectoryTest {

    private NodeBuilder root;

    @Before
    public void setup() {
        root = INITIAL_CONTENT.builder();
    }

    @Test
    public void testDirectoryWritable() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory directory = new OakDirectory(storageBuilder, "testIndex", false);
        // In write mode the directory should accept files directly
        assertNotNull(directory.listAll());
    }

    @Test
    public void testListAllEmpty() throws Exception {
        OakDirectory directory = new OakDirectory(root.child("storageRoot"), "testIndex", false);
        String[] files = directory.listAll();
        assertNotNull(files);
        assertEquals(0, files.length);
    }

    @Test
    public void testWriteAndReadFile() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory directory = new OakDirectory(storageBuilder, "testIndex", false);

        // Write file
        String fileName = "testfile.txt";
        try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
            output.writeString("Hello Lucene 9");
            output.writeLong(123456789L);
        }

        // Verify file exists
        String[] files = directory.listAll();
        assertEquals(1, files.length);
        assertEquals(fileName, files[0]);

        // Read file back
        try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
            assertEquals("Hello Lucene 9", input.readString());
            assertEquals(123456789L, input.readLong());
        }
    }
}
