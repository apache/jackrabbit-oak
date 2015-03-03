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
package org.apache.jackrabbit.oak.spi.security;

import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositeConfigurationTest extends AbstractCompositeConfigurationTest {

    private static final String NAME = "test";

    @Override
    public void before() throws Exception {
        super.before();

        compositeConfiguration = new CompositeConfiguration("test", getSecurityProvider()) {};
    }

    @Test
    public void testGetName() {
        assertEquals(NAME, compositeConfiguration.getName());
    }

    @Test
    public void testEmpty() {
        assertSame(ConfigurationParameters.EMPTY, compositeConfiguration.getParameters());
        assertTrue(compositeConfiguration.getConfigurations().isEmpty());
    }

    @Test
    public void testSetDefaultConfig() {
        SecurityConfiguration sc = new SecurityConfiguration.Default();
        compositeConfiguration.setDefaultConfig(sc);

        List<SecurityConfiguration> configurations = compositeConfiguration.getConfigurations();
        assertFalse(configurations.isEmpty());
        assertEquals(1, configurations.size());
        assertEquals(sc, configurations.iterator().next());
    }

    @Test
    public void testAddConfiguration() {
        compositeConfiguration.addConfiguration(new SecurityConfiguration.Default());
        compositeConfiguration.addConfiguration(new SecurityConfiguration.Default());

        List<SecurityConfiguration> configurations = getConfigurations();
        assertFalse(configurations.isEmpty());
        assertEquals(2, configurations.size());

        SecurityConfiguration def = new SecurityConfiguration.Default();
        compositeConfiguration.setDefaultConfig(def);

        configurations = getConfigurations();
        assertEquals(2, configurations.size());
        assertFalse(configurations.contains(def));
    }

    @Test
    public void testRemoveConfiguration() {
        SecurityConfiguration def = new SecurityConfiguration.Default();
        compositeConfiguration.setDefaultConfig(def);

        SecurityConfiguration sc = new SecurityConfiguration.Default();
        compositeConfiguration.addConfiguration(sc);

        compositeConfiguration.removeConfiguration(def);
        List<SecurityConfiguration> configurations = compositeConfiguration.getConfigurations();
        assertEquals(1, configurations.size());
        assertEquals(sc, configurations.iterator().next());

        compositeConfiguration.removeConfiguration(sc);
        configurations = compositeConfiguration.getConfigurations();
        assertEquals(1, configurations.size());
        assertEquals(def, configurations.iterator().next());
    }
}