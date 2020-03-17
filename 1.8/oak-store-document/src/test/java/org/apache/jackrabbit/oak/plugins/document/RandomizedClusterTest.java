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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Rule;
import org.junit.Test;

import com.mongodb.DB;

/**
 * A simple randomized dual-instance test.
 */
public class RandomizedClusterTest {

    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;

    private static final int MK_COUNT = 2;

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    private DocumentMK[] mkList = new DocumentMK[MK_COUNT];
    private String[] revList = new String[MK_COUNT];
    @SuppressWarnings({ "unchecked", "cast" })
    private HashSet<Integer>[] unseenChanges = (HashSet<Integer>[]) new HashSet[MK_COUNT];
    private HashMap<Integer, List<Op>> changes = new HashMap<Integer, List<Op>>();

    private int opId;

    private int mkId;

    private StringBuilder log;

    /**
     * The map of changes. Key: node name; value: the last operation that
     * changed the node.
     */
    private HashMap<String, Integer> nodeChange = new HashMap<String, Integer>();

    @Test
    public void addRemoveSet() throws Exception {
        for (int i = 0; i < MK_COUNT; i++) {
            unseenChanges[i] = new HashSet<Integer>();
            mkList[i] = createMK(i);
            revList[i] = mkList[i].getHeadRevision();
        }
        HashMap<Integer, ClusterRev> revs =
                new HashMap<Integer, ClusterRev>();

        Random r = new Random(1);
        int operations = 1000, nodeCount = 10;
        int valueCount = 10;
        int maxBackRev = 20;
        log = new StringBuilder();
        try {
            int maskOk = 0, maskFail = 0;
            int opCount = 6;
            nodeChange.clear();
            for (int i = 0; i < operations; i++) {
                opId = i;
                mkId = r.nextInt(mkList.length);
                String node = "t" + r.nextInt(nodeCount);
                String node2 = "t" + r.nextInt(nodeCount);
                String property = "x";
                String value = "" + r.nextInt(valueCount);
                String diff;
                int op = r.nextInt(opCount);
                if (i < 20) {
                    // we need to add many nodes first, so that
                    // there are enough nodes to operate on
                    op = 0;
                }
                String result;
                boolean conflictExpected;
                switch(op) {
                case 0:
                    diff = "+ \"" + node + "\": { \"" + property + "\": " + value + "}";
                    log(diff);
                    if (exists(node)) {
                        log("already exists");
                        result = null;
                    } else {
                        conflictExpected = isConflict(node);
                        result = commit(diff, conflictExpected);
                        if (result != null) {
                            changes.put(i, Arrays.asList(new Op(mkId, node, value)));
                            nodeChange.put(node, i);
                        }
                    }
                    break;
                case 1:
                    diff = "- \"" + node + "\"";
                    log(diff);
                    if (exists(node)) {
                        conflictExpected = isConflict(node);
                        result = commit(diff, conflictExpected);
                        if (result != null) {
                            changes.put(i, Arrays.asList(new Op(mkId, node, null)));
                            nodeChange.put(node, i);
                        }
                    } else {
                        log("doesn't exist");
                        result = null;
                    }
                    break;
                case 2:
                    diff = "^ \"" + node + "/" + property + "\": " + value;
                    log(diff);
                    if (exists(node)) {
                        conflictExpected = isConflict(node);
                        result = commit(diff, conflictExpected);
                        if (result != null) {
                            changes.put(i, Arrays.asList(new Op(mkId, node, value)));
                            nodeChange.put(node, i);
                        }
                    } else {
                        log("doesn't exist");
                        result = null;
                    }
                    break;
                case 3:
                    diff = "> \"" + node + "\": \"" + node2 + "\"";
                    log(diff);
                    if (exists(node) && !exists(node2)) {
                        conflictExpected = isConflict(node) | isConflict(node2);
                        result = commit(diff, conflictExpected);
                        if (result != null) {
                            value = getValue(mkId, i, node);
                            changes.put(i, Arrays.asList(
                                    new Op(mkId, node, null), new Op(mkId, node2, value)));
                            nodeChange.put(node, i);
                            nodeChange.put(node2, i);
                        }
                    } else {
                        log("source doesn't exist or target exists");
                        result = null;
                    }
                    break;
                case 4:
                    if (isConflict(node)) {
                        // the MicroKernelImpl would report a conflict
                        result = null;
                    } else {
                        diff = "* \"" + node + "\": \"" + node2 + "\"";
                        log(diff);
                        if (exists(node) && !exists(node2)) {
                            conflictExpected = isConflict(node2);
                            result = commit(diff, conflictExpected);
                            if (result != null) {
                                value = getValue(mkId, i, node);
                                changes.put(i, Arrays.asList(new Op(mkId, node2, value)));
                                nodeChange.put(node2, i);
                            }
                        } else {
                            log("source doesn't exist or target exists");
                            result = null;
                        }
                    }
                    break;
                case 5:
                    log("sync/refresh");
                    syncAndRefreshAllClusterNodes();
                    // go to head revision
                    result = revList[mkId] = mkList[mkId].getHeadRevision();
                    // fake failure
                    maskFail |= 1 << op;
                    break;
                default:
                    fail();
                    result = null;
                }
                if (result == null) {
                    maskFail |= 1 << op;
                    log(" -> fail " + Integer.toBinaryString(maskFail));
                } else {
                    maskOk |= 1 << op;
                    log(" -> " + result);
                    // all other cluster nodes didn't see this particular change yet
                    for (int j = 0; j < unseenChanges.length; j++) {
                        if (j != mkId) {
                            unseenChanges[j].add(i);
                        }
                    }
                }
                log("get " + node);
                boolean x = get(i, node);
                log("get " + node + " returns " + x);
                log("get " + node2);
                x = get(i, node2);
                log("get " + node2 + " returns " + x);
                DocumentMK mk = mkList[mkId];
                ClusterRev cr = new ClusterRev();
                cr.mkId = mkId;
                cr.rev = mk.getHeadRevision();
                revs.put(i, cr);
                revs.remove(i - maxBackRev);
                log.append('\n');
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
        }
        // System.out.println(log);
        // System.out.println();
    }

    private String getValue(int clusterId, int maxOp, String nodeName) {
        for (int i = maxOp; i >= 0; i--) {
            List<Op> ops = changes.get(i);
            if (ops != null) {
                for (Op o : ops) {
                    if (o.clusterId != clusterId && unseenChanges[clusterId].contains(i)) {
                        continue;
                    }
                    if (o.nodeName.equals(nodeName)) {
                        return o.value;
                    }
                }
            }
        }
        return null;
    }

    private boolean isConflict(String node) {
        Integer change = nodeChange.get(node);
        if (change == null || !unseenChanges[mkId].contains(change)) {
            return false;
        }
        return true;
    }

    private void log(String msg) {
        msg = opId + ": [" + mkId + "] " + msg + "\n";
        log.append(msg);
    }

    private void syncClusterNode() {
        for (int i = 0; i < mkList.length; i++) {
            DocumentMK mk = mkList[i];
            mk.backgroundWrite();
        }
        DocumentMK mk = mkList[mkId];
        mk.backgroundRead();
    }

    private void syncAndRefreshAllClusterNodes() {
        syncClusterNode();
        for (int i = 0; i < mkList.length; i++) {
            DocumentMK mk = mkList[i];
            mk.backgroundRead();
            revList[i] = mk.getHeadRevision();
            unseenChanges[i].clear();
        }
        log("sync");
    }

    private boolean get(int maxOp, String node) {
        String head = revList[mkId];
        return get(maxOp, node, head);
    }

    private boolean exists(String node) {
        String head = revList[mkId];
        DocumentMK mk = mkList[mkId];
        return mk.nodeExists("/" + node, head);
    }

    private boolean get(int maxOp, String node, String head) {
        String p = "/" + node;
        DocumentMK mk = mkList[mkId];
        String value = getValue(mkId, maxOp, node);
        if (value == null) {
            assertFalse("path: " + p + " is supposed to not exist",
                    mk.nodeExists(p, head));
            return false;
        }
        if (!mk.nodeExists(p, head)) {
            assertTrue("path: " + p + " is supposed to exist",
                    mk.nodeExists(p, head));
        }
        String expected = "{\":childNodeCount\":0,\"x\":" + value + "}";
        String result = mk.getNodes(p, head, 0, 0, Integer.MAX_VALUE, null);
        expected = normalize(expected);
        result = normalize(result);
        assertEquals(expected, result);
        return true;
    }

    private static String normalize(String json) {
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        JsonObject o = JsonObject.create(t);
        JsopBuilder w = new JsopBuilder();
        o.toJson(w);
        return w.toString();
    }

    private String commit(String diff, boolean conflictExpected) {
        boolean ok = false;
        DocumentMK mk = mkList[mkId];
        String rev = revList[mkId];
        String result = null;
        String ex = null;
        if (conflictExpected) {
            ok = false;
            ex = "conflict expected";
        } else {
            ok = true;
        }
        if (ok) {
            result = mk.commit("/", diff, rev, null);
            revList[mkId] = result;
        } else {
            // System.out.println("--> fail " + e.toString());
            try {
                mk.commit("/", diff, rev, null);
                fail("Should fail: " + diff + " with " + ex);
            } catch (DocumentStoreException e2) {
                // expected
                revList[mkId] = mk.getHeadRevision();
                // it might have been not a conflict with another cluster node
                // TODO test two cases: conflict with other cluster node
                // (this should auto-synchronize until the given conflict)
                // and conflict with a previous change that was already seen,
                // which shouldn't synchronize
                syncAndRefreshAllClusterNodes();
            }
        }
        return result;
    }

    private DocumentMK createMK(int clusterId) {
        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.setAsyncDelay(0);
        if (MONGO_DB) {
            DB db = connectionFactory.getConnection().getDB();
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
        return builder.setClusterId(clusterId + 1).open();
    }

    /**
     * A revision in a certain cluster node.
     */
    static class ClusterRev {
        int mkId;
        String rev;
    }

    /**
     * An operation.
     */
    static class Op {
        final int clusterId;
        final String nodeName;
        final String value;
        public Op(int clusterId, String nodeName, String value) {
            this.clusterId = clusterId;
            this.nodeName = nodeName;
            this.value = value;
        }
    }

}
