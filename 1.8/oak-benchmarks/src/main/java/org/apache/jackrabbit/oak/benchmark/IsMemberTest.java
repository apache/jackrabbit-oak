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
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

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
 * Benchmark for {@link Group#isMember(Authorizable)} with the following setup:
 * - 10 groups
 * - boolean flag defining if the 10 groups will be nested or not.
 * - configurable number of users that will be added as members.
 *
 * The test setup adds the configured number of users as members to the 10 groups,
 * where the target group is picked randomly. But note that each user is only
 * member of one single group!
 *
 * The test run picks random users and tests for being member of a randomly
 * selected group.
 */
public class IsMemberTest extends AbstractTest {

    private final Random random = new Random();

    private static final String REL_TEST_PATH = "testPath";

    private static final String USER = "user";
    private static final String GROUP = "group";
    private static final int GROUP_CNT = 10;

    private final int numberOfUsers;
    private final boolean nestedGroups;

    private List<String> gPaths = new ArrayList<String>();
    private List<String> uPaths = new ArrayList<String>();

    public IsMemberTest(int numberOfUsers, boolean nestedGroups) {
        this.numberOfUsers = numberOfUsers;
        this.nestedGroups = nestedGroups;
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        Session s = loginAdministrative();
        try {
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            Group gr = userManager.createGroup(new PrincipalImpl(GROUP + 0), REL_TEST_PATH);
            gPaths.add(gr.getPath());
            for (int i = 1; i < GROUP_CNT; i++) {
                Group g = userManager.createGroup(new PrincipalImpl(GROUP + i), REL_TEST_PATH);
                if (nestedGroups) {
                    g.addMember(gr);
                }
                gr = g;
                gPaths.add(gr.getPath());
            }

            int cnt = 0;
            for (int i = 0; i <= numberOfUsers; i++) {
                User u = userManager.createUser(USER + i, null, new PrincipalImpl(USER + i), REL_TEST_PATH);
                uPaths.add(u.getPath());

                getRandomGroup(userManager).addMember(u);

                if (++cnt == 20000) {
                    s.save();
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
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    ConfigurationParameters conf = ConfigurationParameters.of(UserConfiguration.NAME,
                            ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR,
                                    ImportBehavior.NAME_BESTEFFORT));
                    SecurityProvider sp = new SecurityProviderBuilder().with(conf).build();
                    return new Jcr(oak).with(sp);
                }
            });
        } else {
            return super.createRepository(fixture);
        }
    }

    @Override
    public void runTest() throws Exception {
        Session s = null;
        try {
            // use system session login to avoid measuring the login-performance here
            s = systemLogin();
            UserManager userManager = ((JackrabbitSession) s).getUserManager();

            for (int i = 0; i <= 1000; i++) {
                Group g = getRandomGroup(userManager);
                boolean isMember = isMember(g, getRandomUser(userManager));
                //System.out.println(USER + i + " is " + (isMember? "" : "not ")+ "member of " +groupId);
            }
        } catch (RepositoryException e) {
            System.out.println(e.getMessage());
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    protected boolean isMember(Group g, Authorizable member) throws RepositoryException {
        return g.isMember(member);
    }

    private Group getRandomGroup(UserManager uMgr) throws RepositoryException {
        return (Group) uMgr.getAuthorizableByPath(gPaths.get(random.nextInt(GROUP_CNT)));
    }

    private User getRandomUser(UserManager uMgr) throws RepositoryException {
        return (User) uMgr.getAuthorizableByPath(uPaths.get(random.nextInt(numberOfUsers)));
    }
}
