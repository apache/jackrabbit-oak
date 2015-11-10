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

import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.cache.Cache;

public class BroadcastTest {

    @Test
    public void broadcastInMemory() throws Exception {
        broadcast("inMemory", 100);
    }
    
    @Test
    public void broadcastUDP() throws Exception {
        broadcast("udp:sendTo localhost", 50);
    }
    
    @Test
    public void broadcastEncryptedUDP() throws Exception {
        broadcast("udp:group FF7E:230::1234;key test;port 9876;sendTo localhost;aes", 50);
    }
    
    private static void broadcast(String type, int minPercentCorrect) throws Exception {
        FileUtils.deleteDirectory(new File("target/broadcastTest"));
        new File("target/broadcastTest").mkdirs();        
        PersistentCache p1 = new PersistentCache("target/broadcastTest/p1,broadcast=" + type);
        PersistentCache p2 = new PersistentCache("target/broadcastTest/p2,broadcast=" + type);
        Cache<PathRev, StringValue> c1 = openCache(p1);
        Cache<PathRev, StringValue> c2 = openCache(p2);
        String key = "/test" + Math.random();
        PathRev k = new PathRev(key, new Revision(0, 0, 0));
        int correct = 0;
        for (int i = 0; i < 50; i++) {
            c1.put(k, new StringValue("Hello World " + i));
            waitFor(c2, k, 20);
            StringValue v2 = c2.getIfPresent(k);
            if (v2 != null && v2.toString().equals("Hello World " + i)) {
                correct++;
            }
            c2.invalidate(k);
            assertNull(c2.getIfPresent(k));
            waitFor(c1, k, null, 20);
            StringValue v1 = c1.getIfPresent(k);
            if (v1 == null) {
                correct++;
            }
        }
        Assert.assertTrue("min: " + minPercentCorrect + " got: " + correct, 
                correct >= minPercentCorrect);
        p1.close();
        p2.close();
    }
    
    private static boolean waitFor(Callable<Boolean> call, int timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e1) {
                // ignore
            }
            long time = System.currentTimeMillis() - start;
            try {
                if (call.call()) {
                    return true;
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            if (time > timeout) {
                return false;
            }
        }
    }
    
    private static <K, V> boolean waitFor(final Cache<K, V> map, final K key, final V value, int timeout) {
        return waitFor(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                V v = map.getIfPresent(key);
//System.out.println("key " + key + " result " + v + " map " + map);                
                if (value == null) {
                    return  v == null;
                } 
                return value.equals(v);
            }
        }, timeout);
    }
    
    private static <K, V> boolean waitFor(final Cache<K, V> map, final K key, int timeout) {
        return waitFor(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return map.getIfPresent(key) != null;
            }
        }, timeout);
    }
    
    private static Cache<PathRev, StringValue> openCache(PersistentCache p) {
        CacheLIRS<PathRev, StringValue> cache = new CacheLIRS.Builder<PathRev, StringValue>().
                maximumSize(1).build();
        return p.wrap(null,  null,  cache, CacheType.DIFF);        
    }

}
