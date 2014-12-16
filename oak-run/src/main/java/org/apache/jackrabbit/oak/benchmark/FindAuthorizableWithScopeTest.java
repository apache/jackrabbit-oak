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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.util.Text;

public class FindAuthorizableWithScopeTest extends AbstractTest {

    private static final String GROUP_ID = "testGroup";

    private final long numberOfUsers;
    private final boolean setScope;

    private Session adminSession;
    private UserManager userMgr;

    public FindAuthorizableWithScopeTest(long numberOfUsers, boolean setScope) {
        this.numberOfUsers = numberOfUsers;
        this.setScope = setScope;
    }

    @Override
    protected void beforeSuite() throws Exception {
        adminSession = loginWriter();

        userMgr = ((JackrabbitSession) adminSession).getUserManager();
        Group gr = userMgr.createGroup(GROUP_ID, new PrincipalImpl(GROUP_ID), "test");
        for (int i = 0; i < numberOfUsers; i++) {
            User u = userMgr.createUser("testUser" + i, null, new PrincipalImpl("testUser" + i), "test");
            gr.addMember(u);
        }
        adminSession.save();
    }

    @Override
    protected void afterSuite() throws Exception {
        Authorizable gr = userMgr.getAuthorizable(GROUP_ID);
        if (gr != null) {
            Node n = adminSession.getNode(Text.getRelativeParent(gr.getPath(), 1));
            n.remove();
        }
        Authorizable u1 = userMgr.getAuthorizable("testUser0");
        if (u1 != null) {
            Node n = adminSession.getNode(Text.getRelativeParent(u1.getPath(), 1));
            n.remove();
        }
        adminSession.save();
    }

    @Override
    protected void runTest() throws Exception {
        Iterator<Authorizable> result = userMgr.findAuthorizables(createQuery());

        while (result.hasNext()) {
            result.next();
        }
    }

    private Query createQuery() {
        if (setScope) {
            return new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    builder.nameMatches("testUser");
                    builder.setScope(GROUP_ID, true);
                }
            };
        } else {
            return new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    builder.nameMatches("testUser");
                }
            };
        }
    }
}