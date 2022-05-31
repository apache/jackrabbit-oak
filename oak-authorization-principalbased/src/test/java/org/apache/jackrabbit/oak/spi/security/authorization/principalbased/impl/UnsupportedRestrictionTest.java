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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for OAK-9775
 */
public class UnsupportedRestrictionTest extends AbstractPrincipalBasedTest {
    
    private final TestRestrictionProvider rp = new TestRestrictionProvider();

    private JackrabbitAccessControlManager acMgr;
    
    @Override
    public void before() throws Exception {
        super.before();
        
        assertSame(rp, getConfig(AuthorizationConfiguration.class).getRestrictionProvider());

        setupContentTrees(TEST_OAK_PATH);
        
        acMgr = getAccessControlManager(root);

        Principal p = getTestSystemUser().getPrincipal();
        PrincipalPolicyImpl acl = getPrincipalPolicyImpl(p, acMgr);
        assertTrue(acl.addEntry(testJcrPath, privilegesFromNames(JCR_READ), createRestrictions("value1"), Collections.emptyMap()));
        assertTrue(acl.addEntry(testJcrPath, privilegesFromNames(JCR_READ), createRestrictions("value2"), Collections.emptyMap()));
        assertTrue(acl.addEntry(testJcrPath, privilegesFromNames(JCR_WRITE), Collections.emptyMap(), Collections.emptyMap()));
        
        
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        acl = getPrincipalPolicyImpl(p, acMgr);
        assertEquals(3, acl.size());

        // make sure the test-restriction is no longer supported (but not removed from the repo content)
        rp.disable();
        RestrictionProvider provider = getConfig(AuthorizationConfiguration.class).getRestrictionProvider();
        assertSame(rp, provider);
        assertTrue(rp.base instanceof RestrictionProviderImpl);
    }

    @NotNull
    private Map<String, Value> createRestrictions(@NotNull String value) {
        ValueFactory vf = getValueFactory(root);
        return Collections.singletonMap(TestRestrictionProvider.NAME, vf.createValue(value));
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters cp = ConfigurationParameters.of(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, rp);
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, cp);
    }
    
    @Test
    public void testGetPolicies() throws Exception {
        Principal testPrincipal = getTestSystemUser().getPrincipal();
        JackrabbitAccessControlList acl = getPrincipalPolicyImpl(testPrincipal, acMgr);
        assertEquals(1, acl.size());
    }
    
    @Test
    public void testGetEffectivePoliciesByPath() throws Exception {
        for (AccessControlPolicy policy : acMgr.getEffectivePolicies(testJcrPath)) {
            if (policy instanceof PrincipalAccessControlList) {
                assertEquals(1, ((PrincipalAccessControlList) policy).size());
            }
        }
    }
    
    private static final class TestRestrictionProvider implements RestrictionProvider {
        
        private static final String NAME = "testRestriction";
        
        private RestrictionProvider base;

        public TestRestrictionProvider() {
            base = new AbstractRestrictionProvider(Collections.singletonMap(NAME, new RestrictionDefinitionImpl(NAME, Type.STRING, false))) {
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
    }
}