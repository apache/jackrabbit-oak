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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Item;
import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.util.TraversingItemVisitor;

public abstract class AbstractDeepTreeTest extends AbstractTest {

    protected Session adminSession;
    protected Node testRoot;

    protected List<String> allPaths;

    @Override
    protected void beforeSuite() throws Exception {
        adminSession = getRepository().login(getCredentials());
        String name = getClass().getSimpleName();
        Node rn = adminSession.getRootNode();

        long start = System.currentTimeMillis();
        if (!rn.hasNode(name)) {
            testRoot = adminSession.getRootNode().addNode(name, "nt:unstructured");
            InputStream in = getClass().getClassLoader().getResourceAsStream("deepTree.xml");
            adminSession.importXML(testRoot.getPath(), in, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
            adminSession.save();
        } else {
            testRoot = rn.getNode(name);
        }
        System.out.println("Import deep tree: " + (System.currentTimeMillis()-start));

        final List<String> paths = new ArrayList<String>();
        ItemVisitor v = new TraversingItemVisitor.Default() {
            @Override
            protected void entering(Node node, int i) throws RepositoryException {
                paths.add(node.getPath());
                super.entering(node, i);
            }
            @Override
            protected void entering(Property prop, int i) throws RepositoryException {
                paths.add(prop.getPath());
                super.entering(prop, i);
            }
        };
        v.visit(testRoot);
        allPaths = paths;

        System.out.println("All paths: " + allPaths.size());
    }

    @Override
    protected void afterSuite() throws Exception {
        try {
            testRoot.remove();
            adminSession.save();
            adminSession.logout();
        } finally {
            super.afterSuite();
        }
    }

    protected static void randomRead(Session testSession, List<String> allPaths, int cnt, boolean doReport) throws RepositoryException {
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
                } else {
                    propertyCnt++;
                }
            } else {
                noAccess++;
            }
        }
        long end = System.currentTimeMillis();
        if (doReport) {
            System.out.println("Session " + testSession.getUserID() + " reading " + (cnt-noAccess) + " (Nodes: "+ nodeCnt +"; Properties: "+propertyCnt+") completed in " + (end - start));
        }
    }
}