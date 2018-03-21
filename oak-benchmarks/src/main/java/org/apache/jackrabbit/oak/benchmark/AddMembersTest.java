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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
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
 * Test the performance of adding a configured number of members to groups. The
 * following parameters can be used to run the benchmark:
 *
 * - numberOfMembers : the number of members that should be added in the test run
 * - batchSize : the size of the memberID-array to be passed to the addMembers call
 * - importBehavior : the {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}; valid options are "besteffort", "ignore" and "abort"
 *
 * An importbehavior of "ignore" and "abort" will required the members to exist
 * and will resolve each ID to the corresponding authorizble first (different test
 * setup). In case of "besteffort" the member is not resolved to an authorizable.
 */
public class AddMembersTest extends AbstractTest {

    static final String REL_TEST_PATH = "testPath";
    static final String USER = "user";
    static final String GROUP = "group";
    static final int GROUP_CNT = 100;
    static final int DEFAULT_BATCH_SIZE = 1;

    final Random random = new Random();
    final int numberOfMembers;
    final int batchSize;
    private final String importBehavior;

    private final List<String> groupPaths = new ArrayList<>(GROUP_CNT);

    public AddMembersTest(int numberOfMembers, int batchSize, @Nonnull String importBehavior) {
        this.numberOfMembers = numberOfMembers;
        this.batchSize = batchSize;
        this.importBehavior = importBehavior;
    }

    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();

        Session s = loginAdministrative();
        try {
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            for (int i = 0; i <= GROUP_CNT; i++) {
                Group g = userManager.createGroup(new PrincipalImpl(GROUP + i), REL_TEST_PATH);
                groupPaths.add(g.getPath());
            }

            createUsers(userManager);

        } finally {
            s.save();
            s.logout();
        }
        System.out.println("setup done");
    }

    protected void createUsers(@Nonnull UserManager userManager) throws Exception {
        if (!ImportBehavior.NAME_BESTEFFORT.equals(importBehavior)) {
            for (int i = 0; i <= numberOfMembers; i++) {
                String id = USER + i;
                userManager.createUser(id, null, new PrincipalImpl(id), REL_TEST_PATH);
            }
        }
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

            if (!ImportBehavior.NAME_BESTEFFORT.equals(importBehavior)) {
                authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(USER + "0");
                if (authorizable != null) {
                    Node n = s.getNode(Text.getRelativeParent(authorizable.getPath(), 1));
                    n.remove();
                }
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
                    ConfigurationParameters config = ConfigurationParameters.of(UserConfiguration.NAME,
                            ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, importBehavior));
                    SecurityProvider sp = SecurityProviderBuilder.newBuilder().with(config).build();
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
            String groupPath = groupPaths.get(random.nextInt(GROUP_CNT));
            Group g = (Group) userManager.getAuthorizableByPath(groupPath);
            addMembers(userManager, g, s);
        } catch (RepositoryException e) {
            if (s.hasPendingChanges()) {
                s.refresh(false);
            }
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    protected void addMembers(@Nonnull UserManager userManger, @Nonnull Group group, @Nonnull Session s) throws Exception {
        for (int i = 0; i <= numberOfMembers; i++) {
            if (batchSize <= DEFAULT_BATCH_SIZE) {
                group.addMembers(USER + i);
            } else {
                List<String> ids = new ArrayList<String>(batchSize);
                for (int j = 0; j < batchSize && i <= numberOfMembers; j++) {
                    ids.add(USER + i);
                    i++;
                }
                group.addMembers(ids.toArray(new String[ids.size()]));
            }
            s.save();
        }
    }
}
