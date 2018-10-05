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
package org.apache.jackrabbit.oak.upgrade.cli.blob;

import java.io.Closeable;
import java.io.IOException;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import com.google.common.io.Closer;

public class FileDataStoreFactory implements BlobStoreFactory {

    private final String directory;

    private final boolean ignoreMissingBlobs;

    public FileDataStoreFactory(String directory, boolean ignoreMissingBlobs) {
        this.directory = directory;
        this.ignoreMissingBlobs = ignoreMissingBlobs;
    }

    @Override
    public BlobStore create(Closer closer) {
        OakFileDataStore delegate = new OakFileDataStore();
        delegate.setPath(directory);
        delegate.init(null);
        closer.register(asCloseable(delegate));

        if (ignoreMissingBlobs) {
            return new SafeDataStoreBlobStore(delegate);
        } else {
            return new DataStoreBlobStore(delegate);
        }
    }

    private static Closeable asCloseable(final FileDataStore store) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                store.close();
            }
        };
    }

    @Override
    public String toString() {
        return String.format("FileDataStore[%s]", directory);
    }
}
