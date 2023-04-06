/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.segment.test;

import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class TemporaryBlobStore extends ExternalResource {

    private final TemporaryFolder folder;

    private final String name;

    private DataStoreBlobStore store;

    public TemporaryBlobStore(TemporaryFolder folder) {
        this(folder, null);
    }

    public TemporaryBlobStore(TemporaryFolder folder, String name) {
        this.folder = folder;
        this.name = name;
    }

    @Override
    protected void before() throws Throwable {
        FileDataStore fds = new FileDataStore();
        configureDataStore(fds);
        fds.init((name == null ? folder.newFolder() : folder.newFolder(name)).getAbsolutePath());
        store = new DataStoreBlobStore(fds);
    }

    protected void configureDataStore(FileDataStore dataStore) {
        dataStore.setMinRecordLength(4092);
    }

    @Override
    protected void after() {
        try {
            store.close();
        } catch (DataStoreException e) {
            throw new IllegalStateException(e);
        }
    }

    public BlobStore blobStore() {
        return store;
    }

}
