/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProviderAware;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class InitializerTest {

    @Test
    public void testInitializerSegment() throws CommitFailedException, IOException {
        NodeStore store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();

        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);

        SecurityProvider provider = new SecurityProviderBuilder().with(
                ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME,
                        ConfigurationParameters.of(ImmutableMap.of("anonymousId", "anonymous",
                                "adminId", "admin",
                                "usersPath", "/home/users",
                                "groupsPath", "/home/groups",
                                "defaultDepth", "1"))))).build();
        WorkspaceInitializer workspaceInitializer = provider.getConfiguration(UserConfiguration.class).getWorkspaceInitializer();

        if (workspaceInitializer instanceof QueryIndexProviderAware) {
            ((QueryIndexProviderAware) workspaceInitializer).setQueryIndexProvider(
                    new CompositeQueryIndexProvider(new PropertyIndexProvider(), new NodeTypeIndexProvider()));
        }

        workspaceInitializer.initialize(
                builder, "default");
        builder.getNodeState();
    }

}
