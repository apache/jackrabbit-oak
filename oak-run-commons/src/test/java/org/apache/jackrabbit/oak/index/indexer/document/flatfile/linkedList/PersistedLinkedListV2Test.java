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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class PersistedLinkedListV2Test extends FlatFileBufferLinkedListTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setup() throws IOException {
        String fileName = folder.newFile().getAbsolutePath();
        BlobStore blobStore = new MemoryBlobStore();
        NodeStateEntryReader reader = new NodeStateEntryReader(blobStore);
        NodeStateEntryWriter writer = new NodeStateEntryWriter(blobStore);
        list = new PersistedLinkedListV2(fileName, writer, reader, 2, 1);
    }

    @After
    public void tearDown() {
        list.close();
    }

}
