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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ProtectionConfigImplTest extends AbstractProtectionConfigTest {
    
    @Before
    public void before() throws Exception {
        super.before();

        registerProtectionConfig(protectionConfig, PROPERTIES);
    }
    
    @Test
    public void testNotIsProtectedProperty() {
        Tree tree = mockTree("name");
        // matching properties
        for (String name : new String[] {"prop1", "prop2"}) {
            PropertyState property = mockProperty(name);
            assertFalse(protectionConfig.isProtectedProperty(tree, property));
            verify(property).getName();
            verifyNoMoreInteractions(property);
        }
        verifyNoInteractions(tree);
    }

    @Test
    public void testIsProtectedProperty() {
        Tree tree = mockTree("name");
        // not matching
        PropertyState property = mockProperty("anotherName");
        assertTrue(protectionConfig.isProtectedProperty(tree, property));
        
        verify(property).getName();
        verifyNoMoreInteractions(property);
        verifyNoInteractions(tree);
    }

    @Test
    public void testNotIsProtectedTree() {
        for (String name : new String[] {"node1", "node2"}) {
            Tree tree = mockTree(name);
            assertFalse(protectionConfig.isProtectedTree(tree));
            verify(tree).getName();
            verifyNoMoreInteractions(tree);
        }
    }

    @Test
    public void testIsProtectedTree() {
        Tree tree = mockTree("anotherName");
        // not matching
        assertTrue(protectionConfig.isProtectedTree(tree));

        verify(tree).getName();
        verifyNoMoreInteractions(tree);
    }
}