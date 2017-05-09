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

/**
 * Randomly read 1000 items from the deep tree.
 */
public class ReadDeepTreeTest extends AbstractTest {

    protected final boolean runAsAdmin;
    protected final int itemsToRead;
    protected final boolean doReport;

    protected final boolean singleSession;

    protected Session adminSession;
    protected Node testRoot;

    protected Session testSession;

    protected List<String> allPaths = new ArrayList<String>();

    protected ReadDeepTreeTest(boolean runAsAdmin, int itemsToRead, boolean doReport) {
        this(runAsAdmin, itemsToRead, doReport, true);
    }

    public ReadDeepTreeTest(boolean runAsAdmin, int itemsToRead, boolean doReport, boolean singleSession) {
        this.runAsAdmin = runAsAdmin;
        this.itemsToRead = itemsToRead;
        this.doReport = doReport;
        this.singleSession = singleSession;
    }

    @Override
    protected void beforeSuite() throws Exception {
        adminSession = loginWriter();
        createDeepTree();
        testSession = singleSession ? getTestSession() : null;
    }

    protected void createDeepTree() throws Exception {
        Node rn = adminSession.getRootNode();
        allPaths.clear();

        String testNodeName = getTestNodeName();
        long start = System.currentTimeMillis();
        if (!rn.hasNode(testNodeName)) {
            testRoot = adminSession.getRootNode().addNode(testNodeName, "nt:unstructured");
            InputStream in = getClass().getClassLoader().getResourceAsStream(getImportFileName());
            adminSession.importXML(testRoot.getPath(), in, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
            adminSession.save();
        } else {
            testRoot = rn.getNode(testNodeName);
        }
        System.out.println("Import deep tree: " + (System.currentTimeMillis()-start));

        ItemVisitor v = new TraversingItemVisitor.Default() {
            @Override
            protected void entering(Node node, int i) throws RepositoryException {
                visitingNode(node, i);
                super.entering(node, i);
            }
            @Override
            protected void entering(Property prop, int i) throws RepositoryException {
                visitingProperty(prop, i);
                super.entering(prop, i);
            }
        };
        v.visit(testRoot);
        System.out.println("All paths: " + allPaths.size());
    }

    protected String getImportFileName() {
        return "deepTree.xml";
    }

    protected String getTestNodeName() {
        return getClass().getSimpleName() + TEST_ID;
    }

    protected void visitingNode(Node node, int i) throws RepositoryException {
        allPaths.add(node.getPath());
    }

    protected void visitingProperty(Property property, int i) throws RepositoryException {
        allPaths.add(property.getPath());
    }

    @Override
    protected void afterSuite() throws Exception {
        testRoot.remove();
        adminSession.save();
    }

    @Override
    protected void runTest() throws Exception {
        randomRead(testSession, allPaths, itemsToRead);
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
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }

    protected Session getTestSession() {
        if (runAsAdmin) {
            return loginWriter();
        } else {
            return loginAnonymous();
        }
    }

}
