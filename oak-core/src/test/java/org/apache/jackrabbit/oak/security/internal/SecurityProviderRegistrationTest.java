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
package org.apache.jackrabbit.oak.security.internal;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.junit.Test;
import org.osgi.framework.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SecurityProviderRegistrationTest extends AbstractSecurityTest {

    private static final Map<String, Object> PROPS = ImmutableMap.<String, Object>of("prop", "val");

    private SecurityProviderRegistration registration = new SecurityProviderRegistration();

    private static void assertContext(@Nonnull Context context, int expectedSize, @Nonnull Tree tree, boolean isDefined) throws Exception {
        Class c = context.getClass();
        assertTrue(c.getName().endsWith("CompositeContext"));

        Field f = c.getDeclaredField("delegatees");
        f.setAccessible(true);

        if (expectedSize == 0) {
            assertNull(f.get(context));
        } else {
            assertEquals(expectedSize, ((Context[]) f.get(context)).length);
        }

        assertEquals(isDefined, context.definesContextRoot(tree));
        assertEquals(isDefined, context.definesTree(tree));
        assertEquals(isDefined, context.definesProperty(tree, PropertyStates.createProperty("abc", "abc")));
        assertEquals(isDefined, context.definesLocation(TreeLocation.create(tree)));
    }

    @Test
    public void testAuthorizationRanking() throws Exception {
        Field f = registration.getClass().getDeclaredField("authorizationConfiguration");
        f.setAccessible(true);

        AuthorizationConfiguration testAc = new TestAuthorizationConfiguration();
        registration.bindAuthorizationConfiguration(testAc, ConfigurationParameters.EMPTY);

        AuthorizationConfigurationImpl ac = new AuthorizationConfigurationImpl();
        ac.setParameters(ConfigurationParameters.of(CompositeConfiguration.PARAM_RANKING, 500));
        registration.bindAuthorizationConfiguration(ac, PROPS);

        AuthorizationConfiguration testAc2 = new TestAuthorizationConfiguration();
        Map<String, Object> props = ImmutableMap.<String, Object>of(Constants.SERVICE_RANKING, new Integer(100));
        registration.bindAuthorizationConfiguration(testAc2, props);

        CompositeAuthorizationConfiguration cac = (CompositeAuthorizationConfiguration) f.get(registration);

        List<AuthorizationConfiguration> list = cac.getConfigurations();
        assertEquals(3, list.size());

        assertSame(ac, list.get(0));
        assertSame(testAc2, list.get(1));
        assertSame(testAc, list.get(2));
    }

    @Test
    public void testAuthorizationContext() throws Exception {
        Tree t = root.getTree("/");

        Field f = registration.getClass().getDeclaredField("authorizationConfiguration");
        f.setAccessible(true);

        AuthorizationConfiguration ac = new AuthorizationConfigurationImpl();
        registration.bindAuthorizationConfiguration(ac, PROPS);
        CompositeAuthorizationConfiguration cac = (CompositeAuthorizationConfiguration) f.get(registration);
        Context ctx = cac.getContext();
        assertContext(ctx, 1, t, false);

        AuthorizationConfiguration ac1 = new TestAuthorizationConfiguration();
        registration.bindAuthorizationConfiguration(ac1, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 2, t, true);

        AuthorizationConfiguration ac2 = new TestAuthorizationConfiguration();
        registration.bindAuthorizationConfiguration(ac2, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 3, t, true);

        // unbind again:

        registration.unbindAuthorizationConfiguration(ac1, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 2, t, true);

        registration.unbindAuthorizationConfiguration(ac, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 1, t, true);

        registration.unbindAuthorizationConfiguration(ac2, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 0, t, false);
    }

    @Test
    public void testPrincipalContext() throws Exception {
        Tree t = root.getTree("/");

        Field f = registration.getClass().getDeclaredField("principalConfiguration");
        f.setAccessible(true);

        PrincipalConfiguration pc = new PrincipalConfigurationImpl();
        registration.bindPrincipalConfiguration(pc, PROPS);
        CompositePrincipalConfiguration cpc = (CompositePrincipalConfiguration) f.get(registration);
        Context ctx = cpc.getContext();
        // expected size = 0 because PrincipalConfigurationImpl comes with the default ctx
        assertContext(ctx, 0, t, false);

        PrincipalConfiguration pc1 = new TestPrincipalConfiguration();
        registration.bindPrincipalConfiguration(pc1, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        // expected size 1 because the PrincipalConfigurationImpl comes with the default ctx
        assertContext(ctx, 1, t, true);

        PrincipalConfiguration pc2 = new TestPrincipalConfiguration();
        registration.bindPrincipalConfiguration(pc2, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 2, t, true);

        // unbind again:

        registration.unbindPrincipalConfiguration(pc, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 2, t, true);

        registration.unbindPrincipalConfiguration(pc1, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 1, t, true);

        registration.unbindPrincipalConfiguration(pc2, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 0, t, false);
    }

    private class TestAuthorizationConfiguration extends ConfigurationBase implements AuthorizationConfiguration {

        @Nonnull
        @Override
        public AccessControlManager getAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
            return null;
        }

        @Nonnull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return null;
        }

        @Nonnull
        @Override
        public PermissionProvider getPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName, @Nonnull Set<Principal> principals) {
            return null;
        }

        @Nonnull
        @Override
        public Context getContext() {
            return new ContextImpl();
        }
    }

    private class TestPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {
        @Nonnull
        @Override
        public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
            return null;
        }

        @Nonnull
        @Override
        public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
            return null;
        }

        @Nonnull
        @Override
        public Context getContext() {
            return new ContextImpl();
        }
    }

    private static class ContextImpl implements Context {

        @Override
        public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
            return true;
        }

        @Override
        public boolean definesContextRoot(@Nonnull Tree tree) {
            return true;
        }

        @Override
        public boolean definesTree(@Nonnull Tree tree) {
            return true;
        }

        @Override
        public boolean definesLocation(@Nonnull TreeLocation location) {
            return true;
        }

        @Override
        public boolean definesInternal(@Nonnull Tree tree) {
            return true;
        }
    }
}