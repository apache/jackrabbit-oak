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
package org.apache.jackrabbit.oak.benchmark.authentication.external;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.benchmark.AbstractTest;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.tree.impl.RootProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.PrincipalNameResolver;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIDPManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.sling.testing.mock.osgi.context.OsgiContextImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base benchmark test for external authentication.
 *
 * The setup currently defines the following configuration options:
 *
 * - {@code numberOfUsers} : number of user accounts that are 'known' to the IDP
 * - {@code numberOfGroups}: number of groups 'known' to the IDP and equally used to define the membershipSize of each user.
 * - {@code expirationTime}: expiration time as set with
 *   {@link DefaultSyncConfig.Authorizable#setExpirationTime(long)}, used for both users and groups
 * - {@code dynamicMembership}: boolean flag to enable dynamic membership (see OAK-4101)
 *
 * Note: by default the {@link DefaultSyncConfig.User#setMembershipNestingDepth(long)}
 * is set to 1 and each user will become member of each of the groups as defined
 * by {@code numberOfGroups}.
 */
abstract class AbstractExternalTest extends AbstractTest<RepositoryFixture> {

    private static final String PATH_PREFIX = "pathPrefix";

    protected final long seed = Long.getLong("seed", System.currentTimeMillis());
    private final Random random = new Random(seed);
    private final ExternalPrincipalConfiguration externalPrincipalConfiguration = new ExternalPrincipalConfiguration();

    private ContentRepository contentRepository;
    private final SecurityProvider securityProvider;

    final DefaultSyncConfig syncConfig = new DefaultSyncConfig();
    final SyncHandler syncHandler = new DefaultSyncHandler(syncConfig);

    final TestIdentityProvider idp;
    final long delay;
    final long cacheExpiration;

    SyncManagerImpl syncManager;
    ExternalIdentityProviderManager idpManager;

    protected AbstractExternalTest(int numberOfUsers, int numberOfGroups,
                                   long expTime, boolean dynamicMembership,
                                   @NotNull List<String> autoMembership) {
        this(numberOfUsers, numberOfGroups, expTime, dynamicMembership, autoMembership, 0, 0);
    }

    protected AbstractExternalTest(int numberOfUsers, int numberOfGroups,
                                   long expTime, boolean dynamicMembership,
                                   @NotNull List<String> autoMembership,
                                   int roundtripDelay, long cacheExpiration) {

        this.cacheExpiration = cacheExpiration;
        idp = (roundtripDelay < 0) ? new PrincipalResolvingProvider(numberOfUsers, numberOfGroups) : new TestIdentityProvider(numberOfUsers, numberOfGroups);
        delay = roundtripDelay;
        syncConfig.user()
                .setMembershipNestingDepth(1)
                .setDynamicMembership(dynamicMembership)
                .setAutoMembership(autoMembership.toArray(new String[autoMembership.size()]))
                .setExpirationTime(expTime).setPathPrefix(PATH_PREFIX);
        syncConfig.group()
                .setExpirationTime(expTime).setPathPrefix(PATH_PREFIX);
        securityProvider = newTestSecurityProvider(externalPrincipalConfiguration);
        expandSyncConfig();
    }

    protected abstract Configuration createConfiguration();
    
    protected ConfigurationParameters getSecurityConfiguration() {
        if (cacheExpiration > 0) {
            return ConfigurationParameters.of(
                    UserConfiguration.NAME, ConfigurationParameters.of("cacheExpiration", cacheExpiration));
        }
        return ConfigurationParameters.EMPTY;
    }
    
    protected void expandSyncConfig() {
        // nop
    }

    protected ContentRepository getContentRepository() {
        Validate.checkState(contentRepository != null);
        return contentRepository;
    }

    protected SecurityProvider getSecurityProvider() {
        return securityProvider;
    }

    protected String getRandomUserId() {
        int index = random.nextInt(idp.numberOfUsers);
        return "u" + index;
    }

    protected String getRandomGroupId() {
        int index = random.nextInt(idp.membershipSize);
        return "g" + index;
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures, List<Integer> concurrencyLevels) {
        // make sure the desired JAAS config is set
        Configuration.setConfiguration(createConfiguration());
        super.run(fixtures, concurrencyLevels);
    }

    @Override
    protected void beforeSuite() throws Exception {
        Set<String> autoMembership = syncConfig.user().getAutoMembership();
        if (!autoMembership.isEmpty()) {
            Session s = systemLogin();
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            for (String groupId : autoMembership) {
                userManager.createGroup(groupId, new PrincipalImpl(groupId), PATH_PREFIX);
            }
            s.save();
        }
    }

    /**
     * Remove any user/group accounts that have been synchronized into the repo.
     *
     * @throws Exception
     */
    @Override
    protected void afterSuite() throws Exception {
        Session s = systemLogin();
        for (String creationRoot : new String[] {UserConstants.DEFAULT_USER_PATH, UserConstants.DEFAULT_GROUP_PATH}) {
            String path = creationRoot + "/" + PATH_PREFIX;
            if (s.nodeExists(path)) {
                s.getNode(path).remove();
            }
        }
        s.save();
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    OsgiContextImpl context = new OsgiContextImpl();
                    Whiteboard whiteboard = new OsgiWhiteboard(context.bundleContext());
                    oak.with(whiteboard);

                    syncManager = new SyncManagerImpl(whiteboard);
                    whiteboard.register(SyncManager.class, syncManager, Collections.emptyMap());

                    idpManager = new ExternalIDPManagerImpl(whiteboard);
                    whiteboard.register(ExternalIdentityProviderManager.class, idpManager, Collections.emptyMap());

                    whiteboard.register(ExternalIdentityProvider.class, idp, Collections.emptyMap());
                    whiteboard.register(SyncHandler.class, syncHandler, Collections.emptyMap());

                    // assert proper init of the 'externalPrincipalConfiguration' if dynamic membership is enabled
                    if (syncConfig.user().getDynamicMembership()) {
                        
                        // register the userconfiguration in order to have the dynamicmembership provider registered in 
                        // the activate method
                        UserConfiguration uc = securityProvider.getConfiguration(UserConfiguration.class);
                        context.registerInjectActivateService(uc, getSecurityConfiguration().get(UserConfiguration.NAME));

                        // register the ExternalPrincipal configuration in order to have it's
                        // activate method invoked.
                        externalPrincipalConfiguration.setRootProvider(new RootProviderService());
                        externalPrincipalConfiguration.setTreeProvider(new TreeProviderService());
                        context.registerInjectActivateService(externalPrincipalConfiguration);

                        // now register the sync-handler with the dynamic membership config
                        // in order to enable dynamic membership with the external principal configuration
                        Map<String, Object> props = ImmutableMap.of(
                                DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, syncConfig.user().getDynamicMembership(),
                                DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS, syncConfig.group().getDynamicGroups(),
                                DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP, syncConfig.user().getAutoMembership());
                        context.registerService(SyncHandler.class, WhiteboardUtils.getService(whiteboard, SyncHandler.class), props);

                        Map<String, Object> shMappingProps = ImmutableMap.of(
                                SyncHandlerMapping.PARAM_IDP_NAME, idp.getName(),
                                SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME, syncConfig.getName());
                        context.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, shMappingProps);
                    }
                    Jcr jcr = new Jcr(oak).with(securityProvider);
                    contentRepository = jcr.createContentRepository();
                    return jcr;
                }
            });
        } else {
            throw new UnsupportedOperationException("unsupported fixture" + fixture);
        }
    }

    private SecurityProvider newTestSecurityProvider(ExternalPrincipalConfiguration externalPrincipalConfiguration) {

        SecurityProvider delegate = SecurityProviderBuilder.newBuilder().with(getSecurityConfiguration()).build();
    
        PrincipalConfiguration principalConfiguration = delegate.getConfiguration(PrincipalConfiguration.class);
        if (!(principalConfiguration instanceof CompositePrincipalConfiguration)) {
            throw new IllegalStateException();
        } else {
            externalPrincipalConfiguration.setSecurityProvider(delegate);
            CompositePrincipalConfiguration composite = (CompositePrincipalConfiguration) principalConfiguration;
            PrincipalConfiguration defConfig = composite.getDefaultConfig();
            composite.addConfiguration(externalPrincipalConfiguration);
            composite.addConfiguration(defConfig);
        }
        return delegate;
    }

    class TestIdentityProvider implements ExternalIdentityProvider {

        private final int numberOfUsers;
        private final int membershipSize;

        TestIdentityProvider(int numberOfUsers, int membershipSize) {
            this.numberOfUsers = numberOfUsers;
            this.membershipSize = membershipSize;
        }

        @NotNull
        @Override
        public String getName() {
            return "test";
        }

        @Nullable
        @Override
        public ExternalIdentity getIdentity(@NotNull ExternalIdentityRef ref) {
            String id = ref.getId();
            long index = Long.parseLong(id.substring(1));
            if (id.charAt(0) == 'u') {
                return new TestUser(index);
            } else {
                if (delay > 0) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return new TestGroup(index);
            }
        }

        @Nullable
        @Override
        public ExternalUser getUser(@NotNull String userId) {
            return new TestUser(Long.parseLong(userId.substring(1)));
        }

        @Nullable
        @Override
        public ExternalUser authenticate(@NotNull Credentials credentials) {
            return getUser(((SimpleCredentials) credentials).getUserID());
        }

        @Nullable
        @Override
        public ExternalGroup getGroup(@NotNull String name) {
            return new TestGroup(Long.parseLong(name.substring(1)));
        }

        @NotNull
        @Override
        public Iterator<ExternalUser> listUsers() {
            Set<ExternalUser> all = new HashSet<>();
            for (long i = 0; i < numberOfUsers; i++) {
                all.add(new TestUser(i));
            }
            return all.iterator();
        }

        @NotNull
        @Override
        public Iterator<ExternalGroup> listGroups() {
            Set<ExternalGroup> all = new HashSet<>();
            for (long i = 0; i < membershipSize; i++) {
                all.add(new TestGroup(i));
            }
            return all.iterator();
        }

        Iterable<ExternalIdentityRef> getDeclaredGroupRefs(String userId) {
            if (userId.charAt(0) == 'u') {
                Set<ExternalIdentityRef> groupRefs = new HashSet<>();
                for (long i = 0; i < membershipSize; i++) {
                    groupRefs.add(new ExternalIdentityRef("g"+ i, idp.getName()));
                }
                return groupRefs;
            } else {
                return Set.of();
            }
        }
    }

    private final class PrincipalResolvingProvider extends TestIdentityProvider implements PrincipalNameResolver {

        PrincipalResolvingProvider(int numberOfUsers, int membershipSize) {
            super(numberOfUsers, membershipSize);
        }

        @NotNull
        @Override
        public String fromExternalIdentityRef(@NotNull ExternalIdentityRef externalIdentityRef) {
            return "p_" + externalIdentityRef.getId();
        }
    }

    private class TestIdentity implements ExternalIdentity {

        private final String userId;
        private final String principalName;
        private final ExternalIdentityRef id;

        public TestIdentity(@NotNull String userId) {
            this.userId = userId;
            this.principalName = "p_"+userId;
            id = new ExternalIdentityRef(userId, idp.getName());
        }

        @NotNull
        @Override
        public String getId() {
            return userId;
        }

        @NotNull
        @Override
        public String getPrincipalName() {
            return principalName;
        }

        @NotNull
        @Override
        public ExternalIdentityRef getExternalId() {
            return id;
        }

        @Override
        public String getIntermediatePath() {
            return null;
        }

        @NotNull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return idp.getDeclaredGroupRefs(userId);
        }

        @NotNull
        @Override
        public Map<String, ?> getProperties() {
            return ImmutableMap.of();
        }


    }

    private class TestUser extends TestIdentity implements ExternalUser {

        public TestUser(long index) {
            super("u" + index);
        }
    }

    private class TestGroup extends TestIdentity implements ExternalGroup {

        public TestGroup(long index) {
            super("g" + index);
        }

        @NotNull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredMembers() throws ExternalIdentityException {
            return Set.of();
        }
    }
}
