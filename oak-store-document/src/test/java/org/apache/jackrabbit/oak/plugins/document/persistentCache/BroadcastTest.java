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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast.Broadcaster;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast.TCPBroadcaster;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;

import com.google.common.cache.Cache;

public class BroadcastTest {
    
    public static void main(String... args) throws Exception {
        listen();
        benchmark();
    }
    
    private static void benchmark() throws IOException {
        FileUtils.deleteDirectory(new File("target/broadcastTest"));
        new File("target/broadcastTest").mkdirs();     
        String type = "tcp:key 1;ports 9700 9800";
        ArrayList<PersistentCache> nodeList = new ArrayList<PersistentCache>();
        for (int nodes = 1; nodes < 20; nodes++) {
            PersistentCache pc = new PersistentCache("target/broadcastTest/p" + nodes + ",broadcast=" + type);
            Cache<PathRev, StringValue> cache = openCache(pc);
            String key = "/test" + Math.random();
            PathRev k = new PathRev(key, new RevisionVector(new Revision(0, 0, 0)));
            long time = System.currentTimeMillis();
            for (int i = 0; i < 2000; i++) {
                cache.put(k, new StringValue("Hello World " + i));
                cache.invalidate(k);
                cache.getIfPresent(k);
            }
            time = System.currentTimeMillis() - time;
            System.out.println("nodes: " + nodes + " time: " + time);
            nodeList.add(pc);
        }
        for (PersistentCache c : nodeList) {
            c.close();
        }
    }
    
    private static void listen() throws InterruptedException {
        String config = "key 123";

        
        ConsoleAppender<ILoggingEvent> ca = new ConsoleAppender<ILoggingEvent>();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        ca.setContext(lc);
        PatternLayout pl = new PatternLayout();
        pl.setPattern("%msg%n");
        pl.setContext(lc);
        pl.start();
        ca.setLayout(pl);
        ca.start();
        
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(TCPBroadcaster.class);
        logger.addAppender(ca);
        logger.setLevel(Level.DEBUG);
        
        TCPBroadcaster receiver = new TCPBroadcaster(config);
        receiver.addListener(new Broadcaster.Listener() {

            @Override
            public void receive(ByteBuffer buff) {
                int end = buff.position();
                StringBuilder sb = new StringBuilder();
                while (buff.remaining() > 0) {
                    char c = (char) buff.get();
                    if (c >= ' ' && c < 128) {
                        sb.append(c);
                    } else if (c <= 9) {
                        sb.append((char) ('0' + c));
                    } else {
                        sb.append('.');
                    }
                }
                String dateTime = new Timestamp(System.currentTimeMillis()).toString().substring(0, 19);
                System.out.println(dateTime + " Received " + sb);
                buff.position(end);
            }
            
        });
        Random r = new Random();
        int x = r.nextInt();
        System.out.println("Sending " + x);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(10);
            ByteBuffer buff = ByteBuffer.allocate(1024);
            buff.putInt(0);
            buff.putInt(x);
            buff.put(new byte[100]);
            buff.flip();
            receiver.send(buff);
            if (!receiver.isRunning()) {
                System.out.println("Did not start or already stopped");
                break;
            }
        }
        Thread.sleep(Integer.MAX_VALUE);
    }
    
    @Test
    public void broadcastTCP() throws Exception {
        broadcast("tcp:sendTo localhost;key 123", 80);
    }

    @Test
    public void broadcastInMemory() throws Exception {
        broadcast("inMemory", 100);
    }
    
    @Test
    @Ignore("OAK-2843")
    public void broadcastUDP() throws Exception {
        try {
            broadcast("udp:sendTo localhost", 50);
        } catch (AssertionError e) {
            // IPv6 didn't work, so try with IPv4
            try {
                broadcast("udp:group 228.6.7.9", 50);                
            } catch (AssertionError e2) {
                throwBoth(e, e2);
            }                
        }
    }
    
    @Test
    @Ignore("OAK-2843")
    public void broadcastEncryptedUDP() throws Exception {
        try {
            broadcast("udp:group FF78:230::1234;key test;port 9876;sendTo localhost;aes", 50);
        } catch (AssertionError e) {
            try {
                broadcast("udp:group 228.6.7.9;key test;port 9876;aes", 50);                
            } catch (AssertionError e2) {
                throwBoth(e, e2);
            }                
        }
    }
    
    private static void throwBoth(AssertionError e, AssertionError e2) throws AssertionError {
        Throwable ex = e;
        while (ex.getCause() != null) {
            ex = ex.getCause();
        }
        ex.initCause(e2);
        throw e;
    }

    private static void broadcast(String type, int minPercentCorrect)
            throws Exception {
        for (int i = 0; i < 20; i++) {
            if (broadcastTry(type, minPercentCorrect, true)) {
                return;
            }
        }
        broadcastTry(type, minPercentCorrect, false);
    }
    
    private static boolean broadcastTry(String type, int minPercentCorrect, boolean tryOnly) throws Exception {
        FileUtils.deleteDirectory(new File("target/broadcastTest"));
        new File("target/broadcastTest").mkdirs();        
        PersistentCache p1 = new PersistentCache("target/broadcastTest/p1,broadcast=" + type);
        PersistentCache p2 = new PersistentCache("target/broadcastTest/p2,broadcast=" + type);
        Cache<PathRev, StringValue> c1 = openCache(p1);
        Cache<PathRev, StringValue> c2 = openCache(p2);
        String key = "/test" + Math.random();
        PathRev k = new PathRev(key, new RevisionVector(new Revision(0, 0, 0)));
        int correct = 0;
        for (int i = 0; i < 50; i++) {
            c1.put(k, new StringValue("Hello World " + i));
            waitFor(c2, k, 10000);
            StringValue v2 = c2.getIfPresent(k);
            if (v2 != null && v2.toString().equals("Hello World " + i)) {
                correct++;
            }
            c2.invalidate(k);
            assertNull(c2.getIfPresent(k));
            waitFor(c1, k, null, 10000);
            StringValue v1 = c1.getIfPresent(k);
            if (v1 == null) {
                correct++;
            }
        }
        p1.close();
        p2.close();
        if (correct >= minPercentCorrect) {
            return true;
        }
        if (tryOnly) {
            return false;
        }
        Assert.fail("min: " + minPercentCorrect + " got: " + correct);
        return false;
    }
    
    private static boolean waitFor(Callable<Boolean> call, int timeoutInMilliseconds) {
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
            if (time > timeoutInMilliseconds) {
                return false;
            }
        }
    }
    
    private static <K, V> boolean waitFor(final Cache<K, V> map, final K key, final V value, int timeoutInMilliseconds) {
        return waitFor(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                V v = map.getIfPresent(key);
                if (value == null) {
                    return  v == null;
                } 
                return value.equals(v);
            }
        }, timeoutInMilliseconds);
    }
    
    private static <K, V> boolean waitFor(final Cache<K, V> map, final K key, int timeoutInMilliseconds) {
        return waitFor(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return map.getIfPresent(key) != null;
            }
        }, timeoutInMilliseconds);
    }
    
    private static Cache<PathRev, StringValue> openCache(PersistentCache p) {
        CacheLIRS<PathRev, StringValue> cache = new CacheLIRS.Builder<PathRev, StringValue>().
                maximumSize(1).build();
        return p.wrap(null,  null,  cache, CacheType.DIFF);        
    }

}
