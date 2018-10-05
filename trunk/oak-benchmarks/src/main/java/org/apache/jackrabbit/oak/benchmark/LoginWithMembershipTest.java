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
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.util.Text;

/**
 * Measure performance of repository login with the test user being direct or
 * inherited member of a specified number of groups.
 */
public class LoginWithMembershipTest extends AbstractLoginTest {

    private static final String GROUP = "group";

    public static final int NUMBER_OF_GROUPS_DEFAULT = 10;

    private final int numberOfGroups;
    private final boolean nestedGroups;

    public LoginWithMembershipTest(boolean runWithToken, int noIterations, int numberOfGroups, boolean nestedGroups, long expiration) {
        super(USER, runWithToken, noIterations, expiration);

        this.numberOfGroups = numberOfGroups;
        this.nestedGroups = nestedGroups;
    }

    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();

        Session s = loginAdministrative();
        try {
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            Authorizable user = userManager.getAuthorizable(USER);

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
            s.save();
        } finally {
            s.logout();
        }
    }

    @Override
    public void afterSuite() throws Exception {
        Session s = loginAdministrative();
        try {

            Authorizable authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(GROUP);
            if (authorizable != null) {
                Node n = s.getNode(Text.getRelativeParent(authorizable.getPath(), 1));
                n.remove();
            }

            s.save();
        } finally {
            s.logout();
        }
    }

    @Override
    public void runTest() throws RepositoryException {
        Repository repository = getRepository();
        for (int i = 0; i < COUNT; i++) {
            repository.login(getCredentials()).logout();
        }
    }
}