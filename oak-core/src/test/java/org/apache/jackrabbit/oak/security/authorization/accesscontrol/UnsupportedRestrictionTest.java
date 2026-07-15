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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AggregationAware;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for OAK-9775
 */
@RunWith(Parameterized.class)
public class UnsupportedRestrictionTest extends AbstractAccessControlTest {

    private static final String RESTRICTION_NAME = "testRestriction";
    private static final String RESTRICTION_NAME_2 = "mvTestRestriction";

    private final boolean useCompositeRestrictionProvider;
    private final TestRestrictionProvider testRestrictionProvider;
    private final RestrictionProvider rp;
    
    private JackrabbitAccessControlManager acMgr;

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{false, "Singular restriction provider"},
                new Object[]{true, "Composite restriction provider"});
    }

    public UnsupportedRestrictionTest(boolean useCompositeRestrictionProvider, @NotNull String name) {
        this.useCompositeRestrictionProvider = useCompositeRestrictionProvider;
        this.testRestrictionProvider = new TestRestrictionProvider(new RestrictionDefinitionImpl(RESTRICTION_NAME, Type.STRING, false));

        if (useCompositeRestrictionProvider) {
            rp = CompositeRestrictionProvider.newInstance(testRestrictionProvider, new TestRestrictionProvider(new RestrictionDefinitionImpl(RESTRICTION_NAME_2, STRINGS, false)));
        } else {
            rp = testRestrictionProvider;
        }
    }
    
    @Override
    public void before() throws Exception {
        super.before();
        
        assertRestrictionProvider();

        acMgr = getAccessControlManager(root);

        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ), false, createRestrictions("value1")));
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ), false, createRestrictions("value2")));
        
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        assertEquals(2, acl.size());

        // make sure the test-restriction is no longer supported (but restrictions not removed from the repo content)
        testRestrictionProvider.disable();
        assertRestrictionProvider();
        assertTrue(testRestrictionProvider.base instanceof RestrictionProviderImpl);
    }

    private void assertRestrictionProvider() {
        if (useCompositeRestrictionProvider) {
            assertTrue(rp instanceof CompositeRestrictionProvider);
        } else {
            assertSame(rp, getRestrictionProvider());
        }
    }
    
    @NotNull
    private Map<String, Value> createRestrictions(@NotNull String value) {
        ValueFactory vf = getValueFactory(root);
        return Collections.singletonMap(RESTRICTION_NAME, vf.createValue(value));
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters cp = ConfigurationParameters.of(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, rp);
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, cp);
    }
    
    @Test
    public void testUpdateAclForDifferentPrincipal() throws Exception {
        // edit the ACL for a different principal and write it back
        Principal testPrincipal = getTestUser().getPrincipal();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        assertEquals(0, acl.size());
        
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ), true);
        assertEquals(1, acl.size());
        
        acMgr.setPolicy(acl.getPath(), acl);
        // this root.commit resulted in 'OakAccessControl0013: Duplicate ACE found exception' in AccessControlValidator
        // which prior to the fix got read into the ACL without restrictions (resulting in 2 identical entries)
        // adding an entry for a different principal didn't clean it up.
        root.commit();

        acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        assertEquals(1, acl.size());
        assertTrue(acMgr.hasPrivileges(TEST_PATH, Collections.singleton(testPrincipal), privilegesFromNames(JCR_READ)));
    }

    @Test
    public void testUpdateAclForSamePrincipal() throws Exception {
        // edit the ACL for the same principal and write it back
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        assertEquals(0, acl.size());

        acl.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ), true);
        assertEquals(1, acl.size());

        acMgr.setPolicy(acl.getPath(), acl);
        // this root.commit passed as adding an entry for the same principal cleared the 2 ACEs from the setup,
        // which prior to the fix got read into the ACL without restrictions (resulting in 2 identical entries)
        root.commit();

        acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        assertEquals(1, acl.size());
        assertTrue(acMgr.hasPrivileges(TEST_PATH, Collections.singleton(EveryonePrincipal.getInstance()), privilegesFromNames(JCR_READ)));
    }
    
    private static final class TestRestrictionProvider implements RestrictionProvider, AggregationAware {
        
        private AbstractRestrictionProvider base;

        public TestRestrictionProvider(@NotNull RestrictionDefinition definition) {
            base = new AbstractRestrictionProvider(Collections.singletonMap(definition.getName(), definition)) {
                @Override
                public @NotNull RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
                    return RestrictionPattern.EMPTY;
                }

                @Override
                public @NotNull RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
                    return RestrictionPattern.EMPTY;
                }
            };
        }
        
        void disable() {
            base = new RestrictionProviderImpl();
        }

        @Override
        public @NotNull Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
            return base.getSupportedRestrictions(oakPath);
        }

        @Override
        public @NotNull Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value value) throws RepositoryException {
            return base.createRestriction(oakPath, oakName, value);
        }

        @Override
        public @NotNull Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value... values) throws RepositoryException {
            return base.createRestriction(oakPath, oakName, values);
        }

        @Override
        public @NotNull Set<Restriction> readRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) {
            return base.readRestrictions(oakPath, aceTree);
        }

        @Override
        public void writeRestrictions(@Nullable String oakPath, @NotNull Tree aceTree, @NotNull Set<Restriction> restrictions) throws RepositoryException {
            base.writeRestrictions(oakPath, aceTree, restrictions);
        }

        @Override
        public void validateRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) throws RepositoryException {
            base.validateRestrictions(oakPath, aceTree);
        }

        @Override
        public @NotNull RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
            return base.getPattern(oakPath, tree);
        }

        @Override
        public @NotNull RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
            return base.getPattern(oakPath, restrictions);
        }

        @Override
        public void setComposite(@NotNull CompositeRestrictionProvider composite) {
            base.setComposite(composite);
        }
    }
}