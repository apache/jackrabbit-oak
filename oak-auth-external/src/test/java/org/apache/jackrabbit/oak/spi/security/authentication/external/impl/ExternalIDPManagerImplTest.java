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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ExternalIDPManagerImplTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final ExternalIDPManagerImpl externalIDPManager = new ExternalIDPManagerImpl();

    @Before
    public void before() {
        context.registerInjectActivateService(externalIDPManager);
    }

    @Test
    public void testGetProvider() {
        assertNull(externalIDPManager.getProvider("test"));
        assertNull(externalIDPManager.getProvider("unknown"));

        ExternalIdentityProvider provider = when(
            mock(ExternalIdentityProvider.class).getName()).thenReturn("test").getMock();
        context.registerService(ExternalIdentityProvider.class, provider);

        assertSame(provider, externalIDPManager.getProvider("test"));
        assertNull(externalIDPManager.getProvider("unknown"));
    }
}
