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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.PARAM_SYSTEM_PRINCIPAL_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SystemPrincipalConfigTest extends AbstractExternalAuthTest {
    
    private static final String SYSTEM_USER_NAME_1 = "systemUser1";
    private static final String SYSTEM_USER_NAME_2 = "systemUser2";
    private static final String SYSTEM_USER_NAME_NOT_CONFIGURED = "systemUserNotConfigured";

    private final Set<String> systemUserNames;
    
    private String workspaceName;
    private SystemPrincipalConfig systemPrincipalConfig;

    public SystemPrincipalConfigTest(String[] systemUserNames, String name) {
        this.systemUserNames = (systemUserNames == null) ? null : ImmutableSet.copyOf(systemUserNames);
    }

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { null, "Null" },
                new Object[] { new String[0], "Empty names" },
                new Object[] { new String[] {SYSTEM_USER_NAME_1}, "Single name" },
                new Object[] { new String[] {SYSTEM_USER_NAME_1, SYSTEM_USER_NAME_2}, "Multiple names" });
    }
    
    @Override
    public void before() throws Exception {
        super.before();
        context.registerService(SyncHandler.class, new DefaultSyncHandler(), ImmutableMap.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        workspaceName = root.getContentSession().getWorkspaceName();

        systemPrincipalConfig = (systemUserNames == null) ? new SystemPrincipalConfig(Collections.emptySet()) : new SystemPrincipalConfig(systemUserNames);
    }

    @Override
    protected @NotNull Map<String, Object> getExternalPrincipalConfiguration() {
        return Collections.singletonMap(PARAM_SYSTEM_PRINCIPAL_NAMES, systemUserNames);
    }
    
    private void assertIsSystem(@NotNull Set<Principal> principals, boolean expectedIsSystem) throws Exception {
        assertEquals(expectedIsSystem, systemPrincipalConfig.containsSystemPrincipal(principals));
        
        // also verify validator-provider
        List<? extends ValidatorProvider> validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, principals, new MoveTracker());
        assertEquals(1, validatorProviders.size());
        ValidatorProvider vp = validatorProviders.get(0);
        assertTrue(vp instanceof ExternalIdentityValidatorProvider);
        
        Field f = ExternalIdentityValidatorProvider.class.getDeclaredField("isSystem");
        f.setAccessible(true);
        assertEquals(expectedIsSystem, f.get(vp));
    }
    
    @Test
    public void testSystemSubject() throws Exception {
        Set<Principal> principals = Collections.singleton(SystemPrincipal.INSTANCE);
        assertIsSystem(principals, true);
    }

    @Test
    public void testAdminSubject() throws Exception {
        Set<Principal> principals = root.getContentSession().getAuthInfo().getPrincipals();
        assertIsSystem(principals, false);
    }
    
    @Test
    public void testEmptySubject() throws Exception {
        assertIsSystem(Collections.emptySet(), false);
    }

    /**
     * Test regular principals with the configured system-user names => must not be recognized as system subjects.
     */
    @Test
    public void testRegularPrincipalSubject() throws Exception {
        Set<Principal> principals = Collections.singleton(() -> SYSTEM_USER_NAME_1);
        assertIsSystem(principals, false);

        principals = Set.of(new PrincipalImpl(SYSTEM_USER_NAME_2), new PrincipalImpl(SYSTEM_USER_NAME_1));
        assertIsSystem(principals, false);

        principals = Set.of(EveryonePrincipal.getInstance(), new GroupPrincipal() {
            @Override
            public boolean isMember(@NotNull Principal member) {
                return false;
            }

            @Override
            public @NotNull Enumeration<? extends Principal> members() {
                return Iterators.asEnumeration(Collections.emptyIterator());
            }

            @Override
            public String getName() {
                return SYSTEM_USER_NAME_2;
            }
        });
        assertIsSystem(principals, false);
    }

    @Test
    public void testSystemUserSubject() throws Exception {
        Set<Principal> principals = Collections.singleton((SystemUserPrincipal) () -> SYSTEM_USER_NAME_NOT_CONFIGURED);
        assertIsSystem(principals, false);
    }
    
    @Test
    public void testConfiguredSystemUserSubject() throws Exception {
        Set<Principal> principals = Collections.singleton((SystemUserPrincipal) () -> SYSTEM_USER_NAME_1);
        assertIsSystem(principals, configContainsSystemUser(SYSTEM_USER_NAME_1));

        principals = Set.of(EveryonePrincipal.getInstance(), (SystemUserPrincipal) () -> SYSTEM_USER_NAME_2);
        assertIsSystem(principals, configContainsSystemUser(SYSTEM_USER_NAME_2));

        principals = Set.of((SystemUserPrincipal) () -> SYSTEM_USER_NAME_2, (SystemUserPrincipal) () -> SYSTEM_USER_NAME_1);
        assertIsSystem(principals, configContainsSystemUser(SYSTEM_USER_NAME_2, SYSTEM_USER_NAME_1));
    }
    
    private boolean configContainsSystemUser(@NotNull String... systemUserName) {
        if (systemUserNames == null) {
            return false;
        } else {
            for (String name : systemUserName) {
                if (systemUserNames.contains(name)) {
                    return true;
                }
            }
            return false;
        }
    }
}