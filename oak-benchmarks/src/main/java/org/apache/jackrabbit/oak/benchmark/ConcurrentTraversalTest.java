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
package org.apache.jackrabbit.oak.benchmark;

import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.util.TraversingItemVisitor;

/**
 * Concurrently reads random items from the deep tree and traverses the the
 * subtree until {@code MAX_LEVEL} is reached, which is currently set to 10.
 */
public class ConcurrentTraversalTest extends ManyUserReadTest {

    /* number of levels to traverse */
    private static final int MAX_LEVEL = 10;

    protected ConcurrentTraversalTest(boolean runAsAdmin, int itemsToRead, boolean doReport, boolean randomUser) {
        super(runAsAdmin, itemsToRead, doReport, randomUser);
    }

    @Override
    protected void runTest() throws Exception {
        traverse(testSession, itemsToRead);
    }

    protected void traverse(Session testSession, int cnt) throws RepositoryException {
        boolean logout = false;
        if (testSession == null) {
            testSession = getTestSession();
            logout = true;
        }
        try {
            int nodeCnt = 0;
            int propertyCnt = 0;
            int noAccess = 0;
            int size = allPaths.size();
            long start = System.currentTimeMillis();
            for (int i = 0; i < cnt; i++) {
                double rand = size * Math.random();
                int index = (int) Math.floor(rand);
                String path = allPaths.get(index);
                if (testSession.itemExists(path)) {
                    Item item = testSession.getItem(path);
                    Visitor visitor = new Visitor();
                    item.accept(visitor);
                    nodeCnt += visitor.nodeCnt;
                    propertyCnt += visitor.propertyCnt;
                } else {
                    noAccess++;
                }
            }
            long end = System.currentTimeMillis();
            if (doReport) {
                System.out.println("Session " + testSession.getUserID() + " reading " + (cnt-noAccess) + " (Nodes: "+ nodeCnt +"; Properties: "+propertyCnt+") completed in " + (end - start));
            }
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }

    private final class Visitor extends TraversingItemVisitor.Default {

        private long propertyCnt;
        private long nodeCnt;

        public Visitor() {
            super(false, MAX_LEVEL);
        }

        @Override
        protected void entering(Property property, int level) {
            propertyCnt++;
        }

        @Override
        protected void entering(Node node, int level) {
            nodeCnt++;
        }
    }
}
