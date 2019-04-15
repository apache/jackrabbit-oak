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

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.composite.MountInfoProviderService;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.apache.sling.testing.mock.osgi.ReferenceViolationException;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.AbstractPrincipalBasedTest.SUPPORTED_PATH;
import static org.junit.Assert.assertTrue;

public class PrincipalBasedAuthorizationConfigurationOsgiTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();

    @NotNull
    protected PrincipalBasedAuthorizationConfiguration initPrincipalBasedAuthorizationConfiguration() {
        // create instance without binding mandatory references
        return new PrincipalBasedAuthorizationConfiguration();
    }

    @Test(expected = ReferenceViolationException.class)
    public void testMissingMandatoryReferences() {
        context.registerInjectActivateService(pbac, ImmutableMap.of());
    }

    @Test(expected = ReferenceViolationException.class)
    public void testMissingMountInfoProviderReference() {
        context.registerInjectActivateService(new FilterProviderImpl(), ImmutableMap.of("path", SUPPORTED_PATH));
        context.registerInjectActivateService(pbac);
    }

    @Test(expected = ReferenceViolationException.class)
    public void testMissingFilterProviderReference() {
        context.registerInjectActivateService(new MountInfoProviderService());
        context.registerInjectActivateService(pbac, ImmutableMap.of());
    }

    @Test
    public void testMountCollidingWithFilterRoot() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, ImmutableMap.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, ImmutableMap.of("mountedPaths", new String[] {SUPPORTED_PATH + "/some/subtree", "/etc"}));

        try {
            context.registerInjectActivateService(pbac, ImmutableMap.of());
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testMountMatchingFilterRoot() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, ImmutableMap.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, ImmutableMap.of("mountedPaths", new String[] {SUPPORTED_PATH}));

        context.registerInjectActivateService(pbac, ImmutableMap.of());
    }

    @Test
    public void testMountAboveFilterRoot() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, ImmutableMap.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, ImmutableMap.of("mountedPaths", new String[] {Text.getRelativeParent(SUPPORTED_PATH, 1)}));

        context.registerInjectActivateService(pbac, ImmutableMap.of());
    }

    @Test
    public void testMountsElsewhere() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, ImmutableMap.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, ImmutableMap.of("mountedPaths", new String[] {"/etc", "/var/some/mount", UserConstants.DEFAULT_GROUP_PATH}));

        context.registerInjectActivateService(pbac, ImmutableMap.of());
    }
}