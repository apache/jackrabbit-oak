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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Random;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.json.JsonObject;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.DB;

/**
 * A simple randomized single-instance test.
 */
public class RandomizedClusterTest {
    
    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;
    
    private static final int MK_COUNT = 2;
    
    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;
    
    private MongoMK[] mkList = new MongoMK[MK_COUNT];
    private MicroKernelImpl[] mkListGold = new MicroKernelImpl[MK_COUNT];
    private String[] revList = new String[MK_COUNT];
    private String[] revListGold = new String[MK_COUNT];

    private int opId;
    
    private int mkId;
    
    private StringBuilder log;

    @Test
    @Ignore
    public void addRemoveSet() throws Exception {
        MicroKernelImpl mkG = new MicroKernelImpl();
        for (int i = 0; i < MK_COUNT; i++) {
            mkList[i] = createMK(i);
            revList[i] = mkList[i].getHeadRevision();
            mkListGold[i] = mkG;
            revListGold[i] = mkListGold[i].getHeadRevision();
        }
        HashMap<Integer, ClusterRev> revs = 
                new HashMap<Integer, ClusterRev>();
        
        Random r = new Random(1);
        int operations = 1000, nodeCount = 10;
        int propertyCount = 5, valueCount = 10;
        int maxBackRev = 20;
        log = new StringBuilder();
        try {
            int maskOk = 0, maskFail = 0;
            int opCount = 6;
            for (int i = 0; i < operations; i++) {
                opId = i;
                mkId = r.nextInt(mkList.length);
                String node = "t" + r.nextInt(nodeCount);
                String node2 = "t" + r.nextInt(nodeCount);
                String property = "p" + r.nextInt(propertyCount);
                String value = "" + r.nextInt(valueCount);
                String diff;
                int op = r.nextInt(opCount);
                if (i < 20) {
                    // we need to add many nodes first, so that
                    // there are enough nodes to operate on
                    op = 0;
                }
                boolean result;
                switch(op) {
                case 0:
                    diff = "+ \"" + node + "\": { \"" + property + "\": " + value + "}";
                    log(diff);
                    result = commit(diff);
                    break;
                case 1:
                    diff = "- \"" + node + "\"";
                    log(diff);
                    result = commit(diff);
                    break;
                case 2:
                    diff = "^ \"" + node + "/" + property + "\": " + value;
                    log(diff);
                    result = commit(diff);
                    break;
                case 3:
                    diff = "> \"" + node + "\": \"" + node2 + "\"";
                    log(diff);
                    result = commit(diff);
                    break;
                case 4:
                    diff = "* \"" + node + "\": \"" + node2 + "\"";
                    log(diff);
                    result = commit(diff);
                    break;
                case 5:
                    revList[mkId] = mkList[mkId].getHeadRevision();
                    revListGold[mkId] = mkListGold[mkId].getHeadRevision();
                    // fake failure
                    result = i % 2 == 0;
                    break;
                default:
                    fail();
                    result = false;
                }
                if (result) {
                    maskOk |= 1 << op;
                } else {
                    maskFail |= 1 << op;
                }
int todo;                
//                get(node);
//                get(node2);
                MongoMK mk = mkList[mkId];
                MicroKernelImpl mkGold = mkListGold[mkId];
                ClusterRev cr = new ClusterRev();
                cr.mkId = mkId;
                cr.rev = mk.getHeadRevision();
                cr.revGold = mkGold.getHeadRevision();
                revs.put(i, cr);
                revs.remove(i - maxBackRev);
                int revId = i - r.nextInt(maxBackRev);
                cr = revs.get(revId);
                if (cr != null) {
int todo2;                
//                    get(node, cr.revGold, cr.rev);
                }
            }
            if (Integer.bitCount(maskOk) != opCount) {
                fail("Not all operations were at least once successful: " + Integer.toBinaryString(maskOk));
            }
            if (Integer.bitCount(maskFail) != opCount) {
                fail("Not all operations failed at least once: " + Integer.toBinaryString(maskFail));
            }
        } catch (AssertionError e) {
            throw new Exception("log: " + log, e);
        } catch (Exception e) {
            throw new Exception("log: " + log, e);
        }
        for (int i = 0; i < MK_COUNT; i++) {
            mkList[i].dispose();
            mkListGold[i].dispose();
        }
        // System.out.println(log);
        // System.out.println();
    }
    
    private void log(String msg) {
        msg = opId + ": [" + mkId + "] " + msg + "\n";
        log.append(msg);
int test;        
System.out.print(msg);
    }
    
    private void get(String node) {
        String headGold = mkListGold[mkId].getHeadRevision();
        String head = mkList[mkId].getHeadRevision();
        get(node, headGold, head);
    }
        
    private void get(String node, String headGold, String head) {
        String p = "/" + node;
        MicroKernelImpl mkGold = mkListGold[mkId];
        MongoMK mk = mkList[mkId];
        if (!mkGold.nodeExists(p, headGold)) {
            assertFalse(mk.nodeExists(p, head));
            return;
        }
        assertTrue("path: " + p, mk.nodeExists(p, head));
        String resultGold = mkGold.getNodes(p, headGold, 0, 0, Integer.MAX_VALUE, null);
        String result = mk.getNodes(p, head, 0, 0, Integer.MAX_VALUE, null);
        resultGold = normalize(resultGold);
        result = normalize(result);
        assertEquals(resultGold, result);
    }
    
    private static String normalize(String json) {
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        JsonObject o = JsonObject.create(t);
        JsopBuilder w = new JsopBuilder();
        o.toJson(w);
        return w.toString();
    }

    private boolean commit(String diff) {
        boolean ok = false;
        MicroKernelImpl mkGold = mkListGold[mkId];
        String revGold = revListGold[mkId];
        MongoMK mk = mkList[mkId];
        String rev = revList[mkId];
        try {
            mkGold.commit("/", diff, revGold, null);
            ok = true;
        } catch (MicroKernelException e) {
            // System.out.println("--> fail " + e.toString());            
            try {
                mk.commit("/", diff, rev, null);
                fail("Should fail: " + diff + " with exception " + e);
            } catch (MicroKernelException e2) {
                // expected
            }
        }
        if (ok) {
            mk.commit("/", diff, rev, null);
        }
        return ok;
    }
    
    private MongoMK createMK(int clusterId) {
        MongoMK.Builder builder = new MongoMK.Builder();
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
            builder.setMongoDB(db);
        } else {
            if (ds == null) {
                ds = new MemoryDocumentStore();
            }
            if (bs == null) {
                bs = new MemoryBlobStore();
            }
            builder.setDocumentStore(ds).setBlobStore(bs);
        }
        return builder.setClusterId(clusterId).open();
    }
    
    /**
     * A revision in a certain cluster node.
     */
    static class ClusterRev {
        int mkId;
        String rev, revGold;
    }

}
