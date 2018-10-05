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

import java.util.List;
import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Concurrently reads random nodes and it's properties from the deep tree.
 */
public class ConcurrentReadRandomNodeAndItsPropertiesTest extends ReadDeepTreeTest {

    public ConcurrentReadRandomNodeAndItsPropertiesTest(boolean runAsAdmin, int itemsToRead, boolean doReport) {
        super(runAsAdmin, itemsToRead, doReport);
    }

    protected void visitingProperty(Property property, int i) throws RepositoryException {
        // don't remember property paths
    }

    protected void randomRead(Session testSession, List<String> allPaths, int cnt) throws RepositoryException {
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
                    if (item.isNode()) {
                        nodeCnt++;
                        Node n = (Node) item;
                        PropertyIterator it = n.getProperties();
                        while (it.hasNext()) {
                            Property p = it.nextProperty();
                            propertyCnt++;
                        }
                    }
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
}
