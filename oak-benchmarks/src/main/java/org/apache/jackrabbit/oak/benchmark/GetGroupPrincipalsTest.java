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

import java.security.Principal;
import javax.jcr.Node;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.util.Text;

public class GetGroupPrincipalsTest extends AbstractTest {

    private static final String USER = "user";
    private static final String GROUP = "group";

    private final int numberOfGroups;
    private final boolean nestedGroups;

    private String principalName;
    private Session session;

    public GetGroupPrincipalsTest(int numberOfGroups, boolean nestedGroups) {
        this.numberOfGroups = numberOfGroups;
        this.nestedGroups = nestedGroups;
    }

    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();

        session = loginAdministrative();

        UserManager userManager = ((JackrabbitSession) session).getUserManager();
        Authorizable user = userManager.getAuthorizable(USER);
        if (user == null) {
            user = userManager.createUser(USER, USER);
            principalName = user.getPrincipal().getName();
        }

        // make sure we have a least a single group the user is member of.
        Group gr = userManager.createGroup(new PrincipalImpl(GROUP), "test");
        gr.addMember(user);

        for (int i = 1; i < numberOfGroups; i++) {
            Group g = userManager.createGroup(new PrincipalImpl(GROUP + i), "test");
            if (!nestedGroups) {
                g.addMember(user);
            } else {
                g.addMember(gr);
            }
            gr = g;
        }
        session.save();
    }

    @Override
    public void afterSuite() throws Exception {
        UserManager userMgr = ((JackrabbitSession) session).getUserManager();
        Authorizable authorizable = userMgr.getAuthorizable(USER);
        if (authorizable != null) {
            authorizable.remove();
        }

        authorizable = userMgr.getAuthorizable(GROUP);
        if (authorizable != null) {
            Node n = session.getNode(Text.getRelativeParent(authorizable.getPath(), 1));
            n.remove();
        }
        session.save();
    }

    @Override
    public void runTest() throws Exception {
        PrincipalManager principalManager = ((JackrabbitSession) session).getPrincipalManager();
        for (int i = 0; i < 1000; i++) {
            Principal p = principalManager.getPrincipal(principalName);
            PrincipalIterator principals = principalManager.getGroupMembership(p);
//            while (principals.hasNext()) {
//                Principal groupPrincipal = principals.nextPrincipal();
//            }
        }
    }
}
