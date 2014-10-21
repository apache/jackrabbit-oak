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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.io.ByteArrayInputStream;
import java.util.Random;

import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Test;

public class CacheTest {

    @Test
    public void test() throws Exception {
        PersistentCache cache = new PersistentCache("target/cacheTest,size=1,-compress");
        MemoryBlobStore mem = new MemoryBlobStore();
        mem.setBlockSizeMin(100);
        BlobStore b = cache.wrapBlobStore(mem);
        Random r = new Random();
        for(int i=0; i<10000; i++) {
            byte[] data = new byte[100];
            r.nextBytes(data);
            String id = b.writeBlob(new ByteArrayInputStream(data));
            b.readBlob(id, 0, new byte[1], 0, 1);
        }
        cache.close();
    }
    
}
