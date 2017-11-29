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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.ObjectArrays;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.util.Text;

/**
 * Abstract test-base for the various membership related operations (isMember, isDeclaredMember, memberOf, declaredMemberOf)
 * with the following characteristics an config options:
 *
 * - 'numberOfGroups' : number of groups, which each member will become member of
 * - 'nested' : flag to enforce nesting of groups; in effect each group will then become member of the previous group
 * - 'numberOfMembers' : number of users to be created and which will become member of all groups
 *
 * The test run randomly picks one group and one user and performs the
 * member-operation as defined by the subclasses (see 'testMembership')
 */
abstract class MemberBaseTest extends AbstractTest {

    private static final int BATCH_SIZE = 100;
    private static final String REL_TEST_PATH = "testPath";
    private static final String USER = "user";
    private static final String GROUP = "group";

    private final Random random = new Random();

    private final int numberOfGroups;
    private final int numberOfMembers;
    private final boolean nested;

    private final List<String> groupPaths;
    private final List<String> memberPaths;

    public MemberBaseTest(int numberOfGroups, boolean nested, int numberOfMembers) {
        this.numberOfGroups = numberOfGroups;
        this.numberOfMembers = numberOfMembers;
        this.nested = nested;

        groupPaths = new ArrayList<String>(numberOfGroups);
        memberPaths = new ArrayList<String>(numberOfMembers);
    }

    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();

        Session s = loginAdministrative();
        try {
            List<String> memberIds = new ArrayList<String>(numberOfMembers);
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            for (int i = 0; i <= numberOfMembers; i++) {
                User u = userManager.createUser(USER + i, null, new PrincipalImpl(USER + i), REL_TEST_PATH);
                memberPaths.add(u.getPath());
                memberIds.add(USER + i);
            }

            String[] idArray = memberIds.toArray(new String[memberIds.size()]);
            for (int i = 0, j = 1; i <= numberOfGroups; i++, j++) {
                Group g = userManager.createGroup(new PrincipalImpl(GROUP + i), REL_TEST_PATH);
                groupPaths.add(g.getPath());

                if (nested) {
                    g.addMembers(ObjectArrays.concat(idArray, GROUP + j));
                } else {
                    g.addMembers(idArray);
                }
            }
        } finally {
            s.save();
            s.logout();
        }
        System.out.println("setup done");
    }

    @Override
    public void afterSuite() throws Exception {
        Session s = loginAdministrative();
        try {
            Authorizable authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(GROUP + "0");
            if (authorizable != null) {
                Node n = s.getNode(Text.getRelativeParent(authorizable.getPath(), 1));
                n.remove();
            }

            authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(USER + "0");
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
    public void runTest() throws Exception {
        Session s = null;
        try {
            // use system session login to avoid measuring the login-performance here
            s = systemLogin();

            UserManager uMgr = ((JackrabbitSession) s).getUserManager();
            for (int i = 0; i <= 1000; i++) {
                testMembership(
                        (Group) uMgr.getAuthorizableByPath(groupPaths.get(random.nextInt(numberOfGroups))),
                        (User) uMgr.getAuthorizableByPath(memberPaths.get(random.nextInt(numberOfMembers))));
            }
        } catch (RepositoryException e) {
            System.out.println(e.getMessage());
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    ConfigurationParameters conf = ConfigurationParameters.of(UserConfiguration.NAME,
                            ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT));
                    SecurityProvider sp = new SecurityProviderBuilder().with(conf).build();
                    return new Jcr(oak).with(sp);
                }
            });
        } else {
            return super.createRepository(fixture);
        }
    }

    protected abstract void testMembership(@Nonnull Group g, @Nonnull User member) throws Exception;
}