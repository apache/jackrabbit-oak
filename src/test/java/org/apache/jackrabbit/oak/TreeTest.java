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
package org.apache.jackrabbit.oak;

import java.util.Iterator;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Contains tests related to {@link Tree}
 */
public class TreeTest extends AbstractOakTest {

    @Override
    protected ContentRepository createRepository() {
        return createEmptyRepository();
    }
    @Test
    public void orderBefore() throws Exception {
        ContentSession s = createAdminSession();
        try {
            Root r = s.getLatestRoot();
            Tree t = r.getTree("/");
            t.addChild("node1");
            t.addChild("node2");
            t.addChild("node3");
            r.commit();
            t = r.getTree("/");
            t.getChild("node1").orderBefore("node2");
            t.getChild("node3").orderBefore(null);
            checkSequence(t.getChildren().iterator(), "node1", "node2", "node3");
            r.commit();
            // check again after commit
            t = r.getTree("/");
            checkSequence(t.getChildren().iterator(), "node1", "node2", "node3");

            t.getChild("node3").orderBefore("node2");
            checkSequence(t.getChildren().iterator(), "node1", "node3", "node2");
            r.commit();
            t = r.getTree("/");
            checkSequence(t.getChildren().iterator(), "node1", "node3", "node2");

            t.getChild("node1").orderBefore(null);
            checkSequence(t.getChildren().iterator(), "node3", "node2", "node1");
            r.commit();
            t = r.getTree("/");
            checkSequence(t.getChildren().iterator(), "node3", "node2", "node1");

            // TODO :childOrder property invisible?
            //assertEquals("must not have any properties", 0, t.getPropertyCount());
        } finally {
            s.close();
        }
    }

    private void checkSequence(Iterator<Tree> trees, String... names) {
        for (String name : names) {
            assertTrue(trees.hasNext());
            assertEquals("wrong sequence", name, trees.next().getName());
        }
        assertFalse("no more nodes expected", trees.hasNext());
    }
}
