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

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SafeDataStoreBlobStore extends DataStoreBlobStore {

    private static final Logger log = LoggerFactory.getLogger(SafeDataStoreBlobStore.class);

    public SafeDataStoreBlobStore(DataStore delegate) {
        super(delegate);
    }

    @Override
    public InputStream getInputStream(final String encodedBlobId)  {
        try {
            return super.getInputStream(encodedBlobId);
        } catch(IOException e) {
            log.warn("Missing blob: {}", encodedBlobId);
            return new ByteArrayInputStream(new byte[0]);
        }
    }
}