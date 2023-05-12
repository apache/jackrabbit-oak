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
package org.apache.jackrabbit.oak.core;

import java.lang.reflect.Field;

import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.tree.TreeUtil.addChild;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to OAK-10147
 */
public class MoveTest extends AbstractSecurityTest {

    private static final Logger LOG = LoggerFactory.getLogger(MoveTest.class);

    @Test
    public void move() throws Exception {
        Root r = adminSession.getLatestRoot();
        Tree base = getOrCreateTree(r.getTree("/"), "/content/en/foo");
        Tree unrelated = getOrCreateTree(base, "unrelated");
        Tree source = getOrCreateTree(base, "source");
        Tree target = getOrCreateTree(base, "target");
        Tree n = getOrCreateTree(source, "node");
        r.commit();

        String basePath = base.getPath();
        String unrelatedPath = unrelated.getPath();
        String sourcePath = concat(source.getPath(), n.getName());
        String targetPath = concat(target.getPath(), n.getName());
        String nodePath = n.getPath();

        for (int i = 0; i < 100; i++) {
            assertTrue(r.move(sourcePath, targetPath));
            r.commit();
            assertTrue(r.move(targetPath, sourcePath));
            r.commit();
            source.getPath();
            target.getPath();
        }

        LOG.info("pendingMoves for " + basePath + ": " + countMoves(base));
        LOG.info("pendingMoves for " + unrelatedPath + ": " + countMoves(unrelated));
        LOG.info("pendingMoves for " + nodePath + ": " + countMoves(n));

        // number of pendingMoves drops to 1 after a read operation
        unrelated.getPath();
        assertEquals(1, countMoves(unrelated));
    }

    @Test
    public void readMany() throws Exception {
        Root r = adminSession.getLatestRoot();
        Tree t = r.getTree("/");
        // warming up
        for (int i = 0; i < 100_000; i++) {
            getOrCreateTree(t, "/content/en/foo/bar");
        }
        long time = System.currentTimeMillis();
        // measure
        for (int i = 0; i < 1_000_000; i++) {
            getOrCreateTree(t, "/content/en/foo/bar");
        }
        time = System.currentTimeMillis() - time;
        LOG.info("time to read: " + time + " ms.");
    }

    private Tree getOrCreateTree(Tree t, String path)
            throws AccessDeniedException {
        for (String name : PathUtils.elements(path)) {
            if (t.hasChild(name)) {
                t = t.getChild(name);
            } else {
                t = addChild(t, name, NT_UNSTRUCTURED);
            }
        }
        return t;
    }

    private int countMoves(Tree t) throws Exception {
        Field pendingMoves = MutableTree.class.getDeclaredField("pendingMoves");
        pendingMoves.setAccessible(true);
        return countMoves(pendingMoves.get(t));
    }

    private int countMoves(Object move) throws Exception {
        Field m = MutableRoot.Move.class.getDeclaredField("next");
        m.setAccessible(true);
        int i = 0;
        while (move != null) {
            i++;
            move = m.get(move);
        }
        return i;
    }
}
