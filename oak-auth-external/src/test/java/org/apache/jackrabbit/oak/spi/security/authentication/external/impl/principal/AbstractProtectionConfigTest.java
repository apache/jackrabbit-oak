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
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ProtectionConfig;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractProtectionConfigTest extends AbstractExternalAuthTest {

    static final Map<String, String[]> PROPERTIES = Map.of(
            "propertyNames", new String[] {"prop1", "prop2"},
            "nodeNames", new String[] {"node1", "node2"});
    
    final ProtectionConfigImpl protectionConfig = new ProtectionConfigImpl();
    
    void registerProtectionConfig(@NotNull ProtectionConfig config, @NotNull Map<String, String[]> properties) {
        context.registerInjectActivateService(config, properties);
    }

    static Tree mockTree(@NotNull String name) {
        return when(mock(Tree.class).getName()).thenReturn(name).getMock();
    }

    static PropertyState mockProperty(@NotNull String name) {
        return when(mock(PropertyState.class).getName()).thenReturn(name).getMock();
    }
}