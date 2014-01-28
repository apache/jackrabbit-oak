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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Random;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.json.JsonObject;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.junit.Test;

import com.mongodb.DB;

/**
 * A simple randomized single-instance test.
 */
public class RandomizedTest {
    
    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;
    
    private DocumentMK mk;
    private MicroKernelImpl mkGold;
    
    private String commitRev;
    private String commitRevGold;
    
    private StringBuilder log;

    @Test
    public void addRemoveSetMoveCopy() throws Exception {
        addRemoveSetMoveCopy(false);
    }
    
    @Test
    public void addRemoveSetMoveCopyBranchMerge() throws Exception {
        addRemoveSetMoveCopy(true);
    }
    
    private void addRemoveSetMoveCopy(boolean branchMerge) throws Exception {
        mk = createMK();
        mkGold = new MicroKernelImpl();
        HashMap<Integer, String> revsGold = new HashMap<Integer, String>();
        HashMap<Integer, String> revs = new HashMap<Integer, String>();
        Random r = new Random(1);
        int operations = 1000, nodeCount = 10;
        int propertyCount = 5, valueCount = 10;
        int maxBackRev = 20;
        log = new StringBuilder();
        try {
            int maskOk = 0, maskFail = 0;
            int opCount = 5;
            if (branchMerge) {
                opCount = 7;
            }
            for (int i = 0; i < operations; i++) {
                String node = "t" + r.nextInt(nodeCount);
                String node2 = "t" + r.nextInt(nodeCount);
                String property = "p" + r.nextInt(propertyCount);
                String value = "" + r.nextInt(valueCount);
                String diff;
                int op = r.nextInt(opCount);
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
                    if (!branchMerge) {
                        fail();
                    }
                    if (commitRevGold == null) {
                        log("branch");
                        commitRevGold = mkGold.branch(commitRevGold);
                        commitRev = mk.branch(commitRev);
                        result = true;
                    } else {
                        result = false;
                    }
                    break;
                case 6:
                    if (!branchMerge) {
                        fail();
                    }
                    if (commitRevGold != null) {
                        log("merge");
                        mkGold.merge(commitRevGold, null);
                        mk.merge(commitRev, null);
                        commitRevGold = null;
                        commitRev = null;
                        result = true;
                    } else {
                        result = false;
                    }
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
                get(node);
                get(node2);
                String revGold = mkGold.getHeadRevision();
                String rev = mk.getHeadRevision();
                revsGold.put(i, revGold);
                revs.put(i, rev);
                revsGold.remove(i - maxBackRev);
                revs.remove(i - maxBackRev);
                int revId = i - r.nextInt(maxBackRev);
                revGold = revsGold.get(revId);
                if (revGold != null) {
                    rev = revs.get(revId);
                    get(node, revGold, rev);
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
        mk.dispose();
        mkGold.dispose();
        // System.out.println(log);
        // System.out.println();
    }
    
    private void log(String msg) {
        log.append(msg).append('\n');
    }
    
    private void get(String node) {
        String headGold = mkGold.getHeadRevision();
        String head = mk.getHeadRevision();
        get(node, headGold, head);
    }
        
    private void get(String node, String headGold, String head) {
        String p = "/" + node;
        if (!mkGold.nodeExists(p, headGold)) {
            assertFalse(mk.nodeExists(p, head));
            return;
        }
        assertTrue(mk.nodeExists(p, head));
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
        try {
            String r = mkGold.commit("/", diff, commitRevGold, null);
            if (commitRevGold != null) {
                commitRevGold = r;
            }
            ok = true;
        } catch (MicroKernelException e) {
            try {
                mk.commit("/", diff, commitRev, null);
                fail("Should fail: " + diff + " with exception " + e);
            } catch (MicroKernelException e2) {
                // expected
            }
        }
        if (ok) {
            String r = mk.commit("/", diff, commitRev, null);
            if (commitRev != null) {
                commitRev = r;
            }
        }
        return ok;
    }
    
    private static DocumentMK createMK() {
        DocumentMK.Builder builder = new DocumentMK.Builder();
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
            builder.setMongoDB(db);
        }
        return builder.open();
    }

}
