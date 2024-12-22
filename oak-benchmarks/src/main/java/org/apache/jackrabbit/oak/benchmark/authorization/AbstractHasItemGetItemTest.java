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
package org.apache.jackrabbit.oak.benchmark.authorization;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.benchmark.ReadDeepTreeTest;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.jetbrains.annotations.NotNull;

import javax.jcr.Item;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static javax.jcr.security.Privilege.JCR_ALL;

abstract class AbstractHasItemGetItemTest extends ReadDeepTreeTest {

    private final int numberOfACEs;

    private final int numberOfGroups;
    private Subject subject;

    List<Privilege> allPrivileges;
    private Set<String> nodeSet;

    AbstractHasItemGetItemTest(int itemsToRead, int numberOfACEs, int numberOfGroups, boolean doReport) {
        super(false, itemsToRead, doReport, false);

        this.numberOfACEs = numberOfACEs;
        this.numberOfGroups = numberOfGroups;
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        // populate subject
        subject = new Subject();
        UserManager userManager = ((JackrabbitSession) adminSession).getUserManager();
        User user = userManager.createUser("testuser", "pw");
        subject.getPrincipals().add(user.getPrincipal());

        for (int i = 0; i < numberOfGroups; i++) {
            Group gr = userManager.createGroup("group" +i);
            subject.getPrincipals().add(gr.getPrincipal());
        }
        adminSession.save();

        JackrabbitAccessControlManager acMgr = (JackrabbitAccessControlManager) adminSession.getAccessControlManager();

        // grant read at the root for one of the principals
        Privilege[] readPrivs = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ);
        Principal principal = subject.getPrincipals().iterator().next();
        Utils.addEntry(acMgr, principal, PathUtils.ROOT_PATH, readPrivs);

        // create additional ACEs according to benchmark configuration
        allPrivileges = Arrays.asList(acMgr.privilegeFromName(JCR_ALL).getAggregatePrivileges());
        createForEachPrincipal(principal, acMgr, allPrivileges);

        adminSession.save();
        nodeSet = Set.copyOf(nodePaths);
    }

    private void createForEachPrincipal(@NotNull Principal pGrantedRead, @NotNull JackrabbitAccessControlManager acMgr, @NotNull List<Privilege> allPrivileges) throws RepositoryException {
        for (Principal principal : subject.getPrincipals()) {
            int cnt = 0;
            int targetCnt = (principal.getName().equals(pGrantedRead.getName())) ? numberOfACEs-1 : numberOfACEs;
            while (cnt < targetCnt) {
                if (createEntry(acMgr, principal, getRandom(nodePaths), (Privilege[]) Utils.getRandom(allPrivileges, 3).toArray(new Privilege[0]))) {
                    cnt++;
                }
            }
        }
    }
    
    boolean createEntry(@NotNull JackrabbitAccessControlManager acMgr, @NotNull Principal principal, @NotNull String path, 
                        @NotNull Privilege[] privileges) throws RepositoryException {
        return Utils.addEntry(acMgr, principal, path, privileges);
    }

    @Override
    protected void afterSuite() throws Exception {
        try {
            Utils.removePrincipals(subject.getPrincipals(), adminSession);
        }  finally  {
            super.afterSuite();
        }
    }

    @Override
    protected void randomRead(Session testSession, List<String> allPaths, int cnt) throws RepositoryException {
        boolean logout = false;
        if (testSession == null) {
            testSession = getTestSession();
            logout = true;
        }
        try {
            int nodeCnt = 0;
            int propertyCnt = 0;
            int addCnt = 0;
            int noAccess = 0;
            int size = allPaths.size();

            AccessControlManager acMgr = testSession.getAccessControlManager();

            long start = System.currentTimeMillis();
            for (int i = 0; i < cnt; i++) {
                double rand = size * Math.random();
                int index = (int) Math.floor(rand);
                String path = allPaths.get(index);
                if (i % 100 == 0) {
                    addCnt++;
                    additionalOperations(path, testSession, acMgr);
                }
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
                System.out.println("Session " + testSession.getUserID() + " reading " + cnt + " (Nodes: "+ nodeCnt +"; Properties: "+propertyCnt+"; no access: "+noAccess+"; "+ additionalMethodName()+": "+addCnt+") completed in " + (end - start));
            }
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }

    @NotNull
    abstract String additionalMethodName();

    abstract void additionalOperations(@NotNull String path, @NotNull Session s, @NotNull AccessControlManager acMgr);

    @NotNull
    String getAccessControlledPath(@NotNull String path) {
        String np = path;
        if (!nodeSet.contains(path)) {
            int ind = path.indexOf(AccessControlConstants.REP_POLICY);
            if (ind == -1) {
                np = PathUtils.getParentPath(path);
            } else {
                np = path.substring(0, ind);
            }
        }
        return np;
    }

    @NotNull
    @Override
    protected Session getTestSession() {
        return loginSubject(subject);
    }

    @NotNull
    @Override
    protected String getImportFileName() {
        return "deepTree.xml";
    }
}