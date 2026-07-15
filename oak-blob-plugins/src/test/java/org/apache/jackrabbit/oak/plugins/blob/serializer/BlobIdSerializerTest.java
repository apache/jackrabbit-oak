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

package org.apache.jackrabbit.oak.plugins.blob.serializer;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlobIdSerializerTest {
    private BlobStore blobStore = new MemoryBlobStore();

    private BlobIdSerializer serializer = new BlobIdSerializer(blobStore);

    @Test
    public void inMemoryBlob() throws Exception{
        Blob b = new ArrayBasedBlob("hello world".getBytes());

        String value = serializer.serialize(b);
        Blob b2 = serializer.deserialize(value);

        assertTrue(AbstractBlob.equal(b, b2));
    }

    @Test
    public void blobStoreBlob() throws Exception{
        Blob b = new ArrayBasedBlob("hello world".getBytes());

        String value = blobStore.writeBlob(b.getNewStream());

        String serValue = serializer.serialize(new BlobStoreBlob(blobStore, value));
        Blob b2 = serializer.deserialize(serValue);

        assertTrue(AbstractBlob.equal(b, b2));
    }

}