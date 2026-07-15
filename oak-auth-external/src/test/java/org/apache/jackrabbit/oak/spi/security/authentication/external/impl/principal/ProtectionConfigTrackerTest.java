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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ProtectionConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;

public class ProtectionConfigTrackerTest extends AbstractProtectionConfigTest {
    
    private ProtectionConfigTracker tracker;

    @Before
    public void before() throws Exception {
        super.before();
        
        tracker = new ProtectionConfigTracker(context.bundleContext());
        tracker.open();
    }
    
    @After
    public void after() throws Exception {
        try {
            if (tracker != null) {
                tracker.close();
            }
        } finally {
            super.after();
        }
    }
    
    @Test
    public void testNoServices() {
        Tree tree = mockTree("tree");
        assertEquals(ProtectionConfig.DEFAULT.isProtectedTree(tree), tracker.isProtectedTree(tree));
        verifyNoInteractions(tree);
        
        PropertyState property = mockProperty("property");
        assertEquals(ProtectionConfig.DEFAULT.isProtectedProperty(tree, property), tracker.isProtectedProperty(tree, property));
        verifyNoInteractions(tree, property);
    }
    
    @Test
    public void testIsProtectedTreeRegisteredServices() {
        // config 1 with allow-list ('node1', 'node2'), every other name is protected
        registerProtectionConfig(protectionConfig, PROPERTIES);
        // config 2 with deny-list ('protected' is a protected name), which is already covered by the first config
        context.registerService(ProtectionConfig.class, new TestProtectionConfig("protected"), Collections.emptyMap());
        
        for (String name : new String[] {"node1", "node2"}) {
            Tree t = mockTree(name);
            assertFalse(tracker.isProtectedTree(t));
        }
        for (String name : new String[] {"someother", "protected"}) {
            Tree t = mockTree(name);
            assertTrue(tracker.isProtectedTree(t));
        }     
    }
    
    @Test
    public void testIsProtectedPropertyRegisteredServices() {
        // 2 configurations with deny-list 
        context.registerService(ProtectionConfig.class, new TestProtectionConfig("protected1"), Collections.emptyMap());
        context.registerService(ProtectionConfig.class, new TestProtectionConfig("protected2"), Collections.emptyMap());

        for (String name : new String[] {"prop", "someother"}) {
            Tree t = mockTree("tree");
            PropertyState property = mockProperty(name);
            assertFalse(tracker.isProtectedProperty(t, property));
        }

        for (String name : new String[] {"protected1", "protected2"}) {
            Tree t = mockTree("tree");
            PropertyState property = mockProperty(name);
            assertTrue(tracker.isProtectedProperty(t, property));
        } 
    }

    public static final class TestProtectionConfig implements ProtectionConfig {
        
        private final String protectedName;

        private TestProtectionConfig(@NotNull String protectedName) {
            this.protectedName = protectedName;
        }
        @Override
        public boolean isProtectedProperty(@NotNull Tree parent, @NotNull PropertyState property) {
            return protectedName.equals(property.getName());
        }

        @Override
        public boolean isProtectedTree(@NotNull Tree tree) {
            return protectedName.equals(tree.getName());
        }
    }
}