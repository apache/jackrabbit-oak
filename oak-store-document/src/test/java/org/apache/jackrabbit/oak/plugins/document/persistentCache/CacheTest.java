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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import com.google.common.cache.Cache;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Test;

public class CacheTest {

    @Test
    public void recoverIfCorrupt() throws Exception {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        new File("target/cacheTest").mkdirs();
        FileOutputStream out = new FileOutputStream("target/cacheTest/cache-0.data");
        out.write("corrupt".getBytes());
        out.close();
        PersistentCache pCache = new PersistentCache("target/cacheTest");
        CacheLIRS<PathRev, StringValue> cache = new CacheLIRS.Builder<PathRev, StringValue>().
                maximumSize(1).build();
        Cache<PathRev, StringValue> map = pCache.wrap(null,  null,  cache, CacheType.DIFF);
        String largeString = new String(new char[1024 * 1024]);
        for (int counter = 0; counter < 10; counter++) {
            long end = System.currentTimeMillis() + 100;
            while (System.currentTimeMillis() < end) {
                Thread.yield();
            }
            for (int i = 0; i < 100; i++) {
                PathRev k = new PathRev("/" + counter, new RevisionVector(new Revision(0, 0, i)));
                map.getIfPresent(k);
                map.put(k, new StringValue(largeString));
            }
        }
        assertTrue("Exceptions: " + pCache.getExceptionCount(), 
                pCache.getExceptionCount() < 100);
    }
    
    @Test
    public void closeAlways() throws Exception {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        PersistentCache cache = new PersistentCache("target/cacheTest,manualCommit");
        CacheMap<String, String> map = cache.openMap(0, "test", null);
        // break the map by calling interrupt
        Thread.currentThread().interrupt();
        map.put("hello", "world");
        cache.close();
        assertFalse(Thread.interrupted());
    }

    @Test
    public void deleteOldAtStartup() throws Exception {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        new File("target/cacheTest").mkdirs();
        new File("target/cacheTest/cache-0.data").createNewFile();
        new File("target/cacheTest/cache-1.data").createNewFile();
        new File("target/cacheTest/cache-2.data").createNewFile();
        new File("target/cacheTest/cache-3.data").createNewFile();
        PersistentCache cache = new PersistentCache("target/cacheTest");
        cache.close();
        assertFalse(new File("target/cacheTest/cache-0.data").exists());
        assertFalse(new File("target/cacheTest/cache-1.data").exists());
        assertTrue(new File("target/cacheTest/cache-2.data").exists());
        assertTrue(new File("target/cacheTest/cache-3.data").exists());
    }

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
