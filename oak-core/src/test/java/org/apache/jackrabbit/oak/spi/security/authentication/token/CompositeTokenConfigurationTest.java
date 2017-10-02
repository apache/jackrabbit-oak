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
package org.apache.jackrabbit.oak.spi.security.authentication.token;

import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.AbstractCompositeConfigurationTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompositeTokenConfigurationTest extends AbstractCompositeConfigurationTest<TokenConfiguration> {

    @Before
    public void before() throws Exception {
        compositeConfiguration = new CompositeTokenConfiguration(createSecurityProvider());
    }

    private TokenConfiguration createTokenConfiguration() {
        return new TestTokenConfig();
    }

    private SecurityProvider createSecurityProvider() {
        return Mockito.mock(SecurityProvider.class);
    }

    @Test
    public void testEmptyConstructor() {
        TokenConfiguration composite = new CompositeTokenConfiguration();
        assertEquals(TokenConfiguration.NAME, composite.getName());
    }

    @Test
    public void testEmpty() {
        List<TokenConfiguration> configs = getConfigurations();
        assertNotNull(configs);
        assertTrue(configs.isEmpty());
    }

    @Test
    public void testSetDefault() {
        TokenConfiguration tc = createTokenConfiguration();
        setDefault(tc);

        List<TokenConfiguration> configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(1, configs.size());

        addConfiguration(tc);
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(1, configs.size());

        addConfiguration(createTokenConfiguration());
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(2, configs.size());
    }

    @Test
    public void testAddConfiguration() {
        TokenConfiguration tc = createTokenConfiguration();
        addConfiguration(tc);

        List<TokenConfiguration> configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(1, configs.size());

        addConfiguration(tc);
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(2, configs.size());

        addConfiguration(createTokenConfiguration());
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(3, configs.size());
    }

    @Test
    public void testRemoveConfiguration() {
        TokenConfiguration tc = createTokenConfiguration();
        addConfiguration(tc);

        List<TokenConfiguration> configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(1, configs.size());

        removeConfiguration(tc);
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(0, configs.size());
    }

    @Test
    public void testGetTokenProvider() {
        CompositeTokenConfiguration ctc = (CompositeTokenConfiguration) compositeConfiguration;

        Root root = Mockito.mock(Root.class);

        TokenProvider tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertFalse(tp instanceof CompositeTokenProvider);

        TokenConfiguration tc = createTokenConfiguration();
        setDefault(tc);
        tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertFalse(tp instanceof CompositeTokenProvider);

        addConfiguration(tc);
        tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertFalse(tp instanceof CompositeTokenProvider);

        addConfiguration(createTokenConfiguration());
        tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertTrue(tp instanceof CompositeTokenProvider);
    }

    private static final class TestTokenConfig extends ConfigurationBase implements TokenConfiguration {

        @Nonnull
        @Override
        public TokenProvider getTokenProvider(Root root) {
            return Mockito.mock(TokenProvider.class);
        }
    }
}