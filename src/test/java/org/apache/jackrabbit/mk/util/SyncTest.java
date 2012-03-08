/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.mk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.jackrabbit.mk.MultiMkTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the sync util.
 */
@RunWith(Parameterized.class)
public class SyncTest extends MultiMkTestBase {

    public SyncTest(String url) {
        super(url);
    }

    @Test
    public void testIterator() {
        HashSet<String> names = new HashSet<String>();
        for (int i = 0; i < 20; i++) {
            mk.commit("/", "+ \"n" + i + "\": {}", mk.getHeadRevision(), "");
            names.add("n" + i);
        }
        String head = mk.getHeadRevision();
        Iterator<String> it = Sync.getAllChildNodeNames(mk, "/", head, 2);
        while (it.hasNext()) {
            String n = it.next();
            assertTrue(names.remove(n));
        }
        assertEquals(0, names.size());
    }

    @Test
    public void test() {
        doTest(-1);
    }

    @Test
    public void testSmallChildNodeBatchSize() {
        doTest(1);
    }

    private void doTest(int childNodeBatchSize) {
        if (!isSimpleKernel(mk)) {
            // TODO fix test since it incorrectly expects a specific order of child nodes
            return;
        }

        mk.commit("/", "+ \"source\": { \"id\": 1, \"plus\": 0, \"a\": { \"x\": 10, \"y\": 20 }, \"b\": {\"z\": 100}, \"d\":{} }", mk.getHeadRevision(), "");
        Sync sync = new Sync();
        if (childNodeBatchSize > 0) {
            sync.setChildNodesPerBatch(childNodeBatchSize);
        }
        String head = mk.getHeadRevision();
        sync.setSource(mk, head, "/");
        String diff = syncToString(sync);
        assertEquals(
                "add /source\n" +
                "setProperty /source id=1\n" +
                "setProperty /source plus=0\n" +
                "add /source/a\n" +
                "setProperty /source/a x=10\n" +
                "setProperty /source/a y=20\n" +
                "add /source/b\n" +
                "setProperty /source/b z=100\n" +
                "add /source/d\n",
                diff);

        mk.commit("/", "+ \"target\": { \"id\": 2, \"minus\": 0, \"a\": { \"x\": 10 }, \"c\": {} }", mk.getHeadRevision(), "");
        head = mk.getHeadRevision();
        sync.setSource(mk, head, "/source");
        sync.setTarget(mk, head, "/target");
        diff = syncToString(sync);
        assertEquals(
                "setProperty /target id=1\n" +
                "setProperty /target plus=0\n" +
                "setProperty /target minus=null\n" +
                "setProperty /target/a y=20\n" +
                "add /target/b\n" +
                "setProperty /target/b z=100\n" +
                "add /target/d\n" +
                "remove /target/c\n", diff);

        sync.setSource(mk, head, "/notExist");
        sync.setTarget(mk, head, "/target");
        diff = syncToString(sync);
        assertEquals(
                "remove /target\n", diff);

        sync.setSource(mk, head, "/notExist");
        sync.setTarget(mk, head, "/notExist2");
        diff = syncToString(sync);
        assertEquals("", diff);

    }

    private static String syncToString(Sync sync) {
        final StringBuilder buff = new StringBuilder();
        sync.run(new Sync.Handler() {

            @Override
            public void addNode(String targetPath) {
                buff.append("add ").append(targetPath).append('\n');
            }

            @Override
            public void removeNode(String targetPath) {
                buff.append("remove ").append(targetPath).append('\n');
            }

            @Override
            public void setProperty(String targetPath, String property, String value) {
                buff.append("setProperty ").append(targetPath).append(' ').
                        append(property).append('=').append(value).append('\n');
            }
        });
        return buff.toString();

    }

}
