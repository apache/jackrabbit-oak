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
package org.apache.jackrabbit.oak.benchmark.authorization.permission;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.benchmark.ReadDeepTreeTest;
import org.apache.jackrabbit.oak.benchmark.authorization.Utils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.security.Privilege;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static javax.jcr.security.Privilege.JCR_ALL;

public class EagerCacheSizeTest extends ReadDeepTreeTest {

    private static final Logger log = LoggerFactory.getLogger(EagerCacheSizeTest.class);

    private final int numberOfACEs;
    private final int subjectSize;
    private final long eagerCacheSize;
    private Subject subject;

    public EagerCacheSizeTest(int itemsToRead, int repeatedRead,  int numberOfACEs, int subjectSize, long eagerCacheSize, boolean doReport) {
        super(false, itemsToRead, doReport, false, repeatedRead);
        this.numberOfACEs = numberOfACEs;
        this.subjectSize = subjectSize;
        this.eagerCacheSize = eagerCacheSize;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    ConfigurationParameters params = ConfigurationParameters.of("eagerCacheSize", eagerCacheSize);
                    SecurityProvider securityProvider = SecurityProviderBuilder.newBuilder().with(ConfigurationParameters.of(AuthorizationConfiguration.NAME, params)).build();
                    return new Jcr(oak).with(securityProvider);
                }
            });
        } else {
            throw new IllegalArgumentException("Fixture " + fixture + " not supported for this benchmark.");
        }
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        // populate subject
        subject = new Subject();
        UserManager userManager = ((JackrabbitSession) adminSession).getUserManager();
        for (int i = 0; i < subjectSize; i++) {
            User user = userManager.createSystemUser("system_" +i, null);
            subject.getPrincipals().add(user.getPrincipal());
        }
        adminSession.save();

        JackrabbitAccessControlManager acMgr = (JackrabbitAccessControlManager) adminSession.getAccessControlManager();

        // grant read at the root
        Principal principal = subject.getPrincipals().iterator().next();
        Privilege[] readPrivs = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ);
        Utils.addEntry(acMgr, principal, PathUtils.ROOT_PATH, readPrivs);

        // create additional ACEs for each principal in the subject
        List<Privilege> allPrivileges = Arrays.asList(acMgr.privilegeFromName(JCR_ALL).getAggregatePrivileges());
        Iterator<Principal> principalIterator = Iterators.cycle(subject.getPrincipals());
        int cnt = 0;
        while (cnt < numberOfACEs) {
            if (!principalIterator.hasNext()) {
                throw new IllegalStateException("Cannot setup ACE. no principals available.");
            }
            if (Utils.addEntry(acMgr, principalIterator.next(), getRandom(nodePaths), getRandomPrivileges(allPrivileges))) {
                cnt++;
            }
        }
        adminSession.save();
    }

    @NotNull
    private static Privilege[] getRandomPrivileges(@NotNull List<Privilege> allPrivileges) {
        Collections.shuffle(allPrivileges);
        return allPrivileges.subList(0, 3).toArray(new Privilege[0]);
    }

    @Override
    protected void afterSuite() throws Exception {
        try {
            Utils.removePrincipals(subject.getPrincipals(), adminSession);
        }  finally  {
            super.afterSuite();
        }
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

    @NotNull
    @Override
    protected String getTestNodeName() {
        return "EagerCacheSizeTest";
    }

}