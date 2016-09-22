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

package org.apache.jackrabbit.oak.segment.standby.server;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultStandbyBlobReader implements StandbyBlobReader {

    private final Logger log = LoggerFactory.getLogger(DefaultStandbyBlobReader.class);

    private final FileStore store;

    DefaultStandbyBlobReader(FileStore store) {
        this.store = store;
    }

    @Override
    public byte[] readBlob(String blobId) {
        BlobStore blobStore = store.getBlobStore();

        if (blobStore == null) {
            return null;
        }

        byte[] bytes = null;

        try (InputStream s = new BlobStoreBlob(blobStore, blobId).getNewStream()) {
            bytes = IOUtils.toByteArray(s);
        } catch (IOException e) {
            log.warn("Error while reading blob content", e);
        }

        return bytes;
    }

}
