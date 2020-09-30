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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Item;
import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import javax.jcr.util.TraversingItemVisitor;
import java.io.InputStream;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Randomly read 1000 items from the deep tree.
 */
public class ReadDeepTreeTest extends AbstractTest {

    public static final int DEFAULT_ITEMS_TD_READ = 1000;
    public static final int DEFAULT_REPEATED_READ = 1;

    protected final boolean runAsAdmin;
    protected final int itemsToRead;
    protected final int repeatedRead;
    protected final boolean doReport;

    protected final boolean singleSession;

    protected Session adminSession;
    protected Node testRoot;

    protected Session testSession;

    protected List<String> allPaths = new ArrayList<String>();
    protected List<String> nodePaths = new ArrayList<>();

    protected ReadDeepTreeTest(boolean runAsAdmin, int itemsToRead, boolean doReport) {
        this(runAsAdmin, itemsToRead, doReport, true);
    }

    public ReadDeepTreeTest(boolean runAsAdmin, int itemsToRead, boolean doReport, boolean singleSession) {
        this(runAsAdmin, itemsToRead, doReport, singleSession, DEFAULT_REPEATED_READ);
    }

    public ReadDeepTreeTest(boolean runAsAdmin, int itemsToRead, boolean doReport, boolean singleSession, int repeatedRead) {
        this.runAsAdmin = runAsAdmin;
        this.itemsToRead = itemsToRead;
        this.doReport = doReport;
        this.singleSession = singleSession;
        this.repeatedRead = repeatedRead;
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
        String path = node.getPath();
        allPaths.add(path);
        if (!path.contains(AccessControlConstants.REP_POLICY)) {
            nodePaths.add(path);
        }
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
            Cnt accessCnt = new Cnt();
            long start = System.currentTimeMillis();
            for (int i = 0; i < cnt; i++) {
                readItem(testSession, getRandom(allPaths), accessCnt);
            }
            long end = System.currentTimeMillis();
            if (doReport) {
                System.out.println("Session " + testSession.getUserID() + " reading " + (cnt-accessCnt.noAccess) + " (Nodes: "+ accessCnt.nodeCnt +"; Properties: "+accessCnt.propertyCnt+") completed in " + (end - start));
            }
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }

    private void readItem(Session testSession, String path, Cnt cnt) throws RepositoryException {
        for (int i = 0; i < repeatedRead; i++) {
            if (testSession.itemExists(path)) {
                Item item = testSession.getItem(path);
                if (item.isNode()) {
                    cnt.nodeCnt++;
                } else {
                    cnt.propertyCnt++;
                }
            } else {
                cnt.noAccess++;
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

    protected void addPolicy(AccessControlManager acMgr, Node node, Privilege[] privileges, List<Principal> principals) throws RepositoryException {
        addPolicy(acMgr, node, privileges, principals.toArray(new Principal[principals.size()]));
    }

    protected void addPolicy(AccessControlManager acMgr, Node node, Privilege[] privileges, Principal... principals) throws RepositoryException {
        String path = getAccessControllablePath(node);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(node.getSession(), path);
        if (acl != null) {
            for (Principal principal : principals) {
                acl.addAccessControlEntry(principal, privileges);
            }
            acMgr.setPolicy(path, acl);
            node.getSession().save();
        }
    }

    protected static String getAccessControllablePath(Node node) throws RepositoryException {
        String path = node.getPath();
        int level = 0;
        if (node.isNodeType(AccessControlConstants.NT_REP_POLICY)) {
            level = 1;
        } else if (node.isNodeType(AccessControlConstants.NT_REP_ACE)) {
            level = 2;
        } else if (node.isNodeType(AccessControlConstants.NT_REP_RESTRICTIONS)) {
            level = 3;
        }
        if (level > 0) {
            path = Text.getRelativeParent(path, level);
        }
        return path;
    }

    @NotNull
    protected static String getRandom(@NotNull List<String> strings) {
        int index = (int) Math.floor(strings.size() * Math.random());
        return strings.get(index);
    }

    private static final class Cnt {
        private int nodeCnt = 0;
        private int propertyCnt = 0;
        private int noAccess = 0;
    }
}
