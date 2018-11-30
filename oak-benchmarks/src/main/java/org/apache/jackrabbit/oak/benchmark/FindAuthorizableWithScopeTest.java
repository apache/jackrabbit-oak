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

import java.util.Iterator;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.util.Text;

public class FindAuthorizableWithScopeTest extends AbstractTest {

    private static final String GROUP_ID = "testGroup";

    private final long numberOfUsers;
    private final long numberOfMembership;
    private final long maxCount;
    private final boolean setScope;
    private final boolean declaredMembership;
    private final boolean runAsAdmin;

    private JackrabbitSession adminSession;
    private UserManager testUserManager;

    public FindAuthorizableWithScopeTest(long numberOfUsers, long numberOfMembership, int maxCount, boolean setScope, boolean declaredMembership, boolean runAsAdmin) {
        this.numberOfUsers = numberOfUsers;
        this.numberOfMembership = numberOfMembership;
        this.maxCount = maxCount;
        this.setScope = setScope;
        this.declaredMembership = declaredMembership;
        this.runAsAdmin = runAsAdmin;
    }

    @Override
    protected void beforeSuite() throws Exception {
        adminSession = (JackrabbitSession) loginWriter();

        UserManager userManager = adminSession.getUserManager();
        Group gr = userManager.createGroup(GROUP_ID, new PrincipalImpl(GROUP_ID), "test");
        User u = null;
        for (int i = 0; i < numberOfUsers; i++) {
            u = userManager.createUser("testUser" + i, "pw", new PrincipalImpl("testUser" + i), "test");
            gr.addMember(u);
        }

        for (int i = 1; i < numberOfMembership; i++) {
            String id = GROUP_ID + i;
            Group g = userManager.createGroup(id, new PrincipalImpl(id), "test");
            g.addMember(u);
        }

        adminSession.save();

        if (runAsAdmin) {
            testUserManager = userManager;
        } else {
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(adminSession, "/");
            if (acl != null) {
                acl.addEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(adminSession, Privilege.JCR_READ), true);
                adminSession.getAccessControlManager().setPolicy("/", acl);
                adminSession.save();
            }
            Session reader = login(new SimpleCredentials(u.getID(), "pw".toCharArray()));
            testUserManager = ((JackrabbitSession) reader).getUserManager();
        }
    }

    @Override
    protected void afterSuite() throws Exception {
        UserManager userManager = adminSession.getUserManager();
        Authorizable gr = userManager.getAuthorizable(GROUP_ID);
        if (gr != null) {
            Node n = adminSession.getNode(Text.getRelativeParent(gr.getPath(), 1));
            n.remove();
        }
        Authorizable u1 = userManager.getAuthorizable("testUser0");
        if (u1 != null) {
            Node n = adminSession.getNode(Text.getRelativeParent(u1.getPath(), 1));
            n.remove();
        }
        adminSession.save();
    }

    @Override
    protected void runTest() throws Exception {
        Iterator<Authorizable> result = testUserManager.findAuthorizables(createQuery());

        while (result.hasNext()) {
            result.next();
        }
    }

    private Query createQuery() {
        return new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setLimit(0, maxCount);
                builder.nameMatches("testUser%");
                if (setScope) {
                    builder.setScope(GROUP_ID, declaredMembership);
                }
            }
        };
    }
}