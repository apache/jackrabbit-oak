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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.util.List;

import org.apache.jackrabbit.oak.spi.security.AbstractCompositeConfigurationTest;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompositeTokenConfigurationTest extends AbstractCompositeConfigurationTest<TokenConfiguration> {

    @Override
    public void before() throws Exception {
        super.before();
        setCompositeConfiguration(new CompositeTokenConfiguration(getSecurityProvider()));
    }

    @Test
    public void testEmpty() {
        List<TokenConfiguration> configs = getConfigurations();
        assertNotNull(configs);
        assertTrue(configs.isEmpty());
    }

    @Test
    public void testSetDefault() {
        TokenConfigurationImpl tc = new TokenConfigurationImpl(getSecurityProvider());
        setDefault(tc);

        List<TokenConfiguration> configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(1, configs.size());

        addConfiguration(tc);
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(1, configs.size());

        addConfiguration(new TokenConfigurationImpl(getSecurityProvider()));
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(2, configs.size());
    }

    @Test
    public void testAddConfiguration() {
        TokenConfigurationImpl tc = new TokenConfigurationImpl(getSecurityProvider());
        addConfiguration(tc);

        List<TokenConfiguration> configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(1, configs.size());

        addConfiguration(tc);
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(2, configs.size());

        addConfiguration(new TokenConfigurationImpl(getSecurityProvider()));
        configs = getConfigurations();
        assertNotNull(configs);
        assertEquals(3, configs.size());
    }

    @Test
    public void testRemoveConfiguration() {
        TokenConfiguration tc = new TokenConfigurationImpl(getSecurityProvider());
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
        CompositeTokenConfiguration ctc = (CompositeTokenConfiguration) getCompositeConfiguration();

        TokenProvider tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertFalse(tp instanceof CompositeTokenProvider);

        TokenConfiguration tc = new TokenConfigurationImpl(getSecurityProvider());
        setDefault(tc);
        tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertFalse(tp instanceof CompositeTokenProvider);
        assertTrue(tp instanceof TokenProviderImpl);

        addConfiguration(tc);
        tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertFalse(tp instanceof CompositeTokenProvider);
        assertTrue(tp instanceof TokenProviderImpl);

        addConfiguration(new TokenConfigurationImpl(getSecurityProvider()));
        tp = ctc.getTokenProvider(root);
        assertNotNull(tp);
        assertTrue(tp instanceof CompositeTokenProvider);
    }
}