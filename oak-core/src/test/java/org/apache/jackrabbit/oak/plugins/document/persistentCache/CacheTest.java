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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Test;

public class CacheTest {

    @Test
    public void test() throws Exception {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        PersistentCache cache = new PersistentCache("target/cacheTest,size=1,-compress");
        try {
            MemoryBlobStore mem = new MemoryBlobStore();
            mem.setBlockSizeMin(100);
            BlobStore b = cache.wrapBlobStore(mem);
            Random r = new Random();
            for (int i = 0; i < 10000; i++) {
                byte[] data = new byte[100];
                r.nextBytes(data);
                String id = b.writeBlob(new ByteArrayInputStream(data));
                b.readBlob(id, 0, new byte[1], 0, 1);
            }
        } finally {
            cache.close();
        }
    }
    
    @Test
    public void interrupt() throws Exception {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        PersistentCache cache = new PersistentCache("target/cacheTest,size=1,-compress");
        try {
            CacheMap<String, String> m1 = cache.openMap(0, "m1", null);
            CacheMap<String, String> m2 = cache.openMap(0, "test", null);
            
            // the cache file was opened once so far
            assertEquals(1, cache.getOpenCount());
            
            // we store 20 mb of data, to ensure not all data is kept in memory
            String largeString = new String(new char[1024 * 1024]);
            int count = 10;
            for (int i = 0; i < count; i++) {
                m1.put("x" + i, largeString);
                m2.put("y" + i, largeString);
            }

            // interrupt the thread, which will cause the FileChannel
            // to be closed in the next read operation
            Thread.currentThread().interrupt();

            // this will force at least one read operation,
            // which should re-open the maps
            for (int i = 0; i < count; i++) {
                m1.get("x" + i);
                m2.get("y" + i);
            }

            assertEquals(2, cache.getOpenCount());

            // re-opening will clear the interrupt flag
            assertFalse(Thread.interrupted());
            
        } finally {
            cache.close();
        }
    }
    
}
