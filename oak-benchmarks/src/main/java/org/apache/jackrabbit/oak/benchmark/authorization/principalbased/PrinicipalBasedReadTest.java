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
package org.apache.jackrabbit.oak.benchmark.authorization.principalbased;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.benchmark.ReadDeepTreeTest;
import org.apache.jackrabbit.oak.benchmark.authorization.Utils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.composite.MountInfoProviderService;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderHelper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.AggregationFilterImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.FilterProviderImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.PrincipalBasedAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.sling.testing.mock.osgi.context.OsgiContextImpl;
import org.jetbrains.annotations.NotNull;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static javax.jcr.security.Privilege.JCR_ALL;

public class PrinicipalBasedReadTest extends ReadDeepTreeTest {

    private final int numberOfACEs;

    private final int subjectSize;
    private Subject subject;

    private final boolean entriesForEachPrincipal;

    private final boolean testDefault;
    private final String compositionType;
    private final boolean useAggregationFilter;

    public PrinicipalBasedReadTest(int itemsToRead, int numberOfACEs, int subjectSize, boolean entriesForEachPrincipal, boolean testDefault, @NotNull String compositionType, boolean useAggregationFilter, boolean doReport) {
        super(false, itemsToRead, doReport, false);

        this.numberOfACEs = numberOfACEs;
        this.subjectSize = subjectSize;
        this.entriesForEachPrincipal = entriesForEachPrincipal;
        this.testDefault = testDefault;
        this.compositionType = compositionType;
        this.useAggregationFilter = useAggregationFilter;
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
        addEntry(acMgr, principal, PathUtils.ROOT_PATH, readPrivs, testDefault);
        // for AND-composite-evaluation with principal-based: also need to grant on default model (see repository setup below)
        if (!testDefault && CompositeAuthorizationConfiguration.CompositionType.AND == CompositeAuthorizationConfiguration.CompositionType.valueOf(compositionType)) {
            addEntry(acMgr, principal, PathUtils.ROOT_PATH, readPrivs, true);
        }

        // create additional ACEs according to benchmark configuration
        List<Privilege> allPrivileges = Arrays.asList(acMgr.privilegeFromName(JCR_ALL).getAggregatePrivileges());
        if (!entriesForEachPrincipal) {
            createForRotatingPrincipal(acMgr, allPrivileges);
        } else {
            createForEachPrincipal(acMgr, allPrivileges);
        }

        adminSession.save();
    }

    private void createForRotatingPrincipal(@NotNull JackrabbitAccessControlManager acMgr, @NotNull List<Privilege> allPrivileges) throws RepositoryException {
        Iterator<Principal> principalIterator = Iterators.cycle(subject.getPrincipals());
        int cnt = 0;
        while (cnt < numberOfACEs) {
            if (!principalIterator.hasNext()) {
                throw new IllegalStateException("Cannot setup ACE. no principals available.");
            }
            if (addEntry(acMgr, principalIterator.next(), getRandom(nodePaths), getRandomPrivileges(allPrivileges), testDefault)) {
                cnt++;
            }
        }
    }

    private void createForEachPrincipal(@NotNull JackrabbitAccessControlManager acMgr, @NotNull List<Privilege> allPrivileges) throws RepositoryException {
        for (Principal principal : subject.getPrincipals()) {
            int cnt = 0;
            while (cnt < numberOfACEs) {
                if (addEntry(acMgr, principal, getRandom(nodePaths), getRandomPrivileges(allPrivileges), testDefault)) {
                    cnt++;
                }
            }
        }
    }

    @NotNull
    private static Privilege[] getRandomPrivileges(@NotNull List<Privilege> allPrivileges) {
        Collections.shuffle(allPrivileges);
        return allPrivileges.subList(0, 3).toArray(new Privilege[0]);
    }

    private static boolean addEntry(@NotNull JackrabbitAccessControlManager acMgr, @NotNull Principal principal, @NotNull String path, @NotNull Privilege[] privileges, boolean useDefault) throws RepositoryException {
        boolean added = false;
        JackrabbitAccessControlList acl = null;
        if (useDefault) {
            acl = AccessControlUtils.getAccessControlList(acMgr, path);
            if (acl == null) {
                throw new IllegalStateException("No policy to setup ACE.");
            }
            added = acl.addAccessControlEntry(principal, privileges);
        } else {
            for (JackrabbitAccessControlPolicy policy : Iterables.concat(ImmutableList.copyOf(acMgr.getApplicablePolicies(principal)), ImmutableList.copyOf(acMgr.getPolicies(principal)))) {
                if (policy instanceof PrincipalAccessControlList) {
                    acl = (PrincipalAccessControlList) policy;
                    break;
                }
            }
            if (acl == null) {
                throw new IllegalStateException("No principal policy to setup ACE.");
            }
            added = ((PrincipalAccessControlList) acl).addEntry(path, privileges);
        }
        if (added) {
            acMgr.setPolicy(acl.getPath(), acl);
        }
        return added;
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
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, oak -> new Jcr(oak).with(createSecurityProvider()));
        } else {
            throw new IllegalArgumentException("Fixture " + fixture + " not supported for this benchmark.");
        }
    }

    @NotNull
    @Override
    protected String getImportFileName() {
        return "deepTree.xml";
    }

    @NotNull
    @Override
    protected String getTestNodeName() {
        return "PrinicipalBasedReadTest";
    }

    private SecurityProvider createSecurityProvider() {
        SecurityProvider delegate = SecurityProviderBuilder.newBuilder().build();
        CompositeAuthorizationConfiguration authorizationConfiguration = (CompositeAuthorizationConfiguration) delegate
                .getConfiguration((AuthorizationConfiguration.class));
        authorizationConfiguration.withCompositionType(compositionType);
        AuthorizationConfiguration defaultAuthorization = requireNonNull(authorizationConfiguration.getDefaultConfig());
        if (testDefault) {
            authorizationConfiguration.addConfiguration(defaultAuthorization);
        } else {
            PrincipalBasedAuthorizationConfiguration pbConfiguration = new PrincipalBasedAuthorizationConfiguration();
            // NOTE: this sets up a ANDing-composite with prinipal-based and default
            //       if 'stop-evaluation' is configured the latter will be called for principal-based evaluation
            SecurityProviderHelper.updateConfig(delegate, pbConfiguration, AuthorizationConfiguration.class);

            OsgiContextImpl context = new OsgiContextImpl();
            // register the filter provider to get it's activate method invoked
            Map<String, Object> props = Map.of("path", PathUtils.concat(UserConstants.DEFAULT_USER_PATH, UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH));
            context.registerInjectActivateService(new FilterProviderImpl(), props);

            // register mountinfo-provider
            context.registerInjectActivateService(new MountInfoProviderService());

            if (useAggregationFilter) {
                authorizationConfiguration.withAggregationFilter(new AggregationFilterImpl());
            }

            // register the authorization configuration to have filterprovider bound to it.
            context.registerInjectActivateService(pbConfiguration);
        }
        return delegate;
    }
}