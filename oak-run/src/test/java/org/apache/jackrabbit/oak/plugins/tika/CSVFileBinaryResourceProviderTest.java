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

package org.apache.jackrabbit.oak.plugins.tika;

import java.io.File;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVPrinter;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class CSVFileBinaryResourceProviderTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void testGetBinaries() throws Exception {
        StringBuilder sb = new StringBuilder();
        CSVPrinter p = new CSVPrinter(sb, CSVFileBinaryResourceProvider.FORMAT);
        // BLOB_ID, LENGTH, JCR_MIMETYPE, JCR_ENCODING, JCR_PATH
        p.printRecord("a", 123, "text/plain", null, "/a");
        p.printRecord("a2", 123, "text/plain", null, "/a/c");
        p.printRecord("b", null, "text/plain", null, "/b");
        p.printRecord(null, null, "text/plain", null, "/c");

        File dataFile = temporaryFolder.newFile();
        Files.write(sb, dataFile, Charsets.UTF_8);

        CSVFileBinaryResourceProvider provider = new CSVFileBinaryResourceProvider(dataFile, new MemoryBlobStore());

        Map<String, BinaryResource> binaries = provider.getBinaries("/").uniqueIndex(BinarySourceMapper.BY_BLOBID);
        assertEquals(3, binaries.size());
        assertEquals("a", binaries.get("a").getBlobId());
        assertEquals("/a", binaries.get("a").getPath());

        binaries = provider.getBinaries("/a").uniqueIndex(BinarySourceMapper.BY_BLOBID);
        assertEquals(1, binaries.size());

        provider.close();
    }
}