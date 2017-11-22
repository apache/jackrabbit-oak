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

package org.apache.jackrabbit.oak.run.cli;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class ReadOnlyBlobStoreWrapperTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void readOnly() throws Exception{
        FileDataStore fds = new FileDataStore();
        fds.setPath(temporaryFolder.getRoot().getAbsolutePath());
        fds.init(null);

        DataStoreBlobStore writableBS = new DataStoreBlobStore(fds);

        BlobStore readOnly = ReadOnlyBlobStoreWrapper.wrap(writableBS);

        try {
            readOnly.writeBlob(new ByteArrayInputStream("foo".getBytes()));
            fail();
        } catch (Exception ignore) {

        }

        String blobId = writableBS.writeBlob(new ByteArrayInputStream("foo".getBytes()));

        try(InputStream is = readOnly.getInputStream(blobId)) {
            assertNotNull(is);
        }

    }

}