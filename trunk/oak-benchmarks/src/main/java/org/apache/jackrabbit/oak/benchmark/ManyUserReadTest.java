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

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

/**
 * UserTest... TODO
 */
public class ManyUserReadTest extends ReadDeepTreeTest {

    private final int numberOfUsers = 1000;
    private final int numberOfMembers = 10;
    private final boolean randomUser;

    protected ManyUserReadTest(boolean runAsAdmin, int itemsToRead, boolean doReport, boolean randomUser) {
        super(runAsAdmin, itemsToRead, doReport, !randomUser);
        this.randomUser = randomUser;
    }

    @Override
    protected void createDeepTree() throws Exception {
        super.createDeepTree();

        UserManager userManager = ((JackrabbitSession) adminSession).getUserManager();
        for (int i = 0; i < numberOfUsers; i++) {
            User user = userManager.createUser("user"+i, "user"+i);
            AccessControlUtils.addAccessControlEntry(adminSession, user.getPath(), user.getPrincipal(), new String[] {Privilege.JCR_ALL}, true);

            Node userNode = adminSession.getNode(user.getPath());
            Node n = userNode.addNode("public");
            n.setProperty("prop", "value");
            String path = n.getPath();
            AccessControlUtils.addAccessControlEntry(adminSession, path, EveryonePrincipal.getInstance(), new String[]{Privilege.JCR_READ}, true);
            allPaths.add(path);
            allPaths.add(path + "/prop");

            Group g = userManager.createGroup("group" + i);
            AccessControlUtils.addAccessControlEntry(adminSession, g.getPath(), g.getPrincipal(), new String[] {Privilege.JCR_READ}, true);

            n = userNode.addNode("semi");
            n.setProperty("prop", "value");
            path = n.getPath();
            AccessControlUtils.addAccessControlEntry(adminSession, path, g.getPrincipal(), new String[]{Privilege.JCR_READ}, true);
            allPaths.add(path);
            allPaths.add(path + "/prop");

            userNode.addNode("private").setProperty("prop", "value");
            adminSession.save();
        }
        System.out.println("Setup "+numberOfUsers+" users");

        for (int i = 0; i < numberOfUsers; i++) {
            Group g = (Group) userManager.getAuthorizable("group"+i);
            for (int j = 0; j < numberOfMembers; j++) {
                g.addMember(userManager.getAuthorizable("user" + getIndex()));
            }
            adminSession.save();
        }

        System.out.println("Setup group membership ("+numberOfMembers+" members per group)");
        System.out.println("All Paths : " + allPaths.size());

        AccessControlUtils.denyAllToEveryone(adminSession, "/rep:security/rep:authorizables");
        adminSession.save();

    }

    @Override
    protected String getImportFileName() {
        return "deepTree_everyone.xml";
    }

    protected Session getTestSession() {
        if (runAsAdmin) {
            return loginWriter();
        } else {
            String userId = (randomUser) ? "user"+getIndex() : "user1";
            SimpleCredentials sc = new SimpleCredentials(userId, userId.toCharArray());
            return login(sc);
        }
    }

    private int getIndex() {
        return (int) Math.floor(numberOfUsers * Math.random());
    }
}