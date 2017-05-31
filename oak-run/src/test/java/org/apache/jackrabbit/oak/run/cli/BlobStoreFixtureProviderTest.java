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
import java.io.IOException;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TypedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BlobStoreFixtureProviderTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void fileDataStore() throws Exception{
        String[] args = {"--fds-path", temporaryFolder.getRoot().getAbsolutePath(), "--read-write"};
        try (BlobStoreFixture fixture = BlobStoreFixtureProvider.create(createFDSOptions(args))){
            String blobId = fixture.getBlobStore().writeBlob(new ByteArrayInputStream("foo".getBytes()));
            assertNotNull(blobId);
        }
    }

    @Test
    public void readOnlyFileDataStore() throws Exception{
        String[] args = {"--fds-path", temporaryFolder.getRoot().getAbsolutePath()};
        try (BlobStoreFixture fixture = BlobStoreFixtureProvider.create(createFDSOptions(args))){
            try {
                BlobStore blobStore = fixture.getBlobStore();

                assertThat(blobStore, instanceOf(GarbageCollectableBlobStore.class));
                assertThat(blobStore, instanceOf(TypedDataStore.class));
                assertThat(blobStore, instanceOf(BlobTrackingStore.class));

                fixture.getBlobStore().writeBlob(new ByteArrayInputStream("foo".getBytes()));
                fail();
            } catch (Exception ignore) {

            }

        }
    }

    private Options createFDSOptions(String... args) throws IOException {
        OptionParser parser = new OptionParser();
        Options opts = new Options().withDisableSystemExit();
        opts.parseAndConfigure(parser, args);
        return opts;
    }
}