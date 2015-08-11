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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.Arrays;
import java.util.List;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.plugins.document.DiffCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MONGO;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDiffCache.Diff;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the MongoDiffCache.
 */
public class MongoDiffCacheTest {

    @Test
    public void diff() {
        Revision from = Revision.fromString("r1-0-1");
        Revision to = Revision.fromString("r2-0-1");
        Diff diff = new Diff(from, to);
        diff.append("/", "^\"foo\":{}");
        diff.append("/foo", "^\"bar\":{}");
        diff.append("/foo/bar", "-\"qux\"");

        assertEquals("^\"foo\":{}", diff.getChanges("/"));
        assertEquals("^\"bar\":{}", diff.getChanges("/foo"));
        assertEquals("-\"qux\"", diff.getChanges("/foo/bar"));
        assertEquals("", diff.getChanges("/baz"));
    }

    @Test
    public void merge() {
        assertEquals("+", doMerge("+", ""));
        assertEquals("-", doMerge("-", ""));
        assertEquals("^", doMerge("^", ""));

        assertEquals("+", doMerge("+"));
        assertEquals("^", doMerge("-", "+"));
        assertEquals("^", doMerge("^", "-", "+"));
        assertEquals("+", doMerge("+", "^", "-", "+"));

        assertEquals("-", doMerge("-"));
        assertEquals("-", doMerge("^", "-"));
        assertEquals("", doMerge("+", "^", "-"));
        assertEquals("-", doMerge("-", "+", "^", "-"));

        assertEquals("^", doMerge("^"));
        assertEquals("+", doMerge("+", "^"));
        assertEquals("^", doMerge("-", "+", "^"));
        assertEquals("^", doMerge("^", "-", "+", "^"));
    }

    @Test
    public void sizeLimit() {
        assumeTrue(MONGO.isAvailable());
        DocumentStore store = MONGO.createDocumentStore();
        assertTrue(store instanceof MongoDocumentStore);
        DB db = ((MongoDocumentStore) store).getDBCollection(NODES).getDB();
        
        MongoDiffCache diffCache = new MongoDiffCache(db, 32, new DocumentMK.Builder());
        DiffCache.Entry entry = diffCache.newEntry(
                new Revision(1, 0, 1), new Revision(2, 0, 1), false);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                for (int k = 0; k < 64; k++) {
                    entry.append("/node-" + i + "/node-" + j + "/node-" + k, 
                            "^\"foo\":{}");
                }
            }
        }
        assertFalse(entry.done());
        
        store.dispose();
    }

    private String doMerge(String... ops) {
        List<String> opsList = Arrays.asList(ops);
        Diff diff = null;
        for (int i = opsList.size() - 1; i >= 0; i--) {
            String op = opsList.get(i);
            if (diff == null) {
                diff = diffFromOp(op);
            } else {
                diff.mergeBeforeDiff(diffFromOp(op));
            }
        }
        if (diff == null) {
            return null;
        }
        String changes = diff.getChanges("/test");
        if (changes == null) {
            return null;
        } else if (changes.length() == 0) {
            return "";
        } else {
            return changes.substring(0, 1);
        }
    }

    private static String changeFromOp(String op) {
        if (op.length() == 0) {
            return "";
        }
        String changes = op + "\"child\"";
        if (!op.equals("-")) {
            changes += ":{}";
        }
        return changes;
    }

    private static Diff diffFromOp(String op) {
        Revision from = Revision.fromString("r1-0-1");
        Revision to = Revision.fromString("r2-0-1");
        Diff d = new Diff(from, to);
        d.append("/test", changeFromOp(op));
        return d;
    }
}
