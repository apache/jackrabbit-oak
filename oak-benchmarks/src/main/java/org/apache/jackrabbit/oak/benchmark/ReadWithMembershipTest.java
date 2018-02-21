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
import java.util.UUID;
import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

public class ReadWithMembershipTest extends ReadDeepTreeTest {

    private final String userId;
    private final int membershipSize;
    private final int numberOfAces;

    protected ReadWithMembershipTest(int itemsToRead, boolean doReport, int membershipSize, int numberOfAces) {
        super(false, itemsToRead, doReport, false);
        userId = "user-" + UUID.randomUUID();
        this.membershipSize = membershipSize;
        this.numberOfAces = numberOfAces;
    }

    @Override
    protected void createDeepTree() throws Exception {
        super.createDeepTree();

        UserManager userManager = ((JackrabbitSession) adminSession).getUserManager();
        User user = userManager.createUser(userId, userId);

        for (int i = 0; i < membershipSize; i++) {
            Group g = userManager.createGroup("group" + i);
            g.addMember(user);

            setupACEs(g.getPrincipal());
        }
        adminSession.save();
    }

    private void setupACEs(Principal principal) throws RepositoryException {
        int size = allPaths.size();
        for (int i = 0; i < numberOfAces; i++) {
            int index = (int) Math.floor(size * Math.random());
            Item item = adminSession.getItem(allPaths.get(index));
            Node n = (item.isNode()) ? (Node) item : item.getParent();
            String path = getAccessControllablePath(n);
            AccessControlUtils.addAccessControlEntry(adminSession, path, principal, new String[] {PrivilegeConstants.JCR_READ}, true);
        }
    }

    @Override
    protected String getImportFileName() {
        return "deepTree_everyone.xml";
    }

    protected Session getTestSession() {
        SimpleCredentials sc = new SimpleCredentials(userId, userId.toCharArray());
        return login(sc);
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    ConfigurationParameters params = ConfigurationParameters.of("eagerCacheSize", 100);
                    SecurityProvider securityProvider = new SecurityProviderBuilder().with(ConfigurationParameters.of(AuthorizationConfiguration.NAME, params)).build();
                    return new Jcr(oak).with(securityProvider);
                }
            });
        } else {
            throw new IllegalArgumentException("Fixture " + fixture + " not supported for this benchmark.");
        }
    }
}