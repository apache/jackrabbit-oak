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
package org.apache.jackrabbit.oak.plugins.document;

import com.google.common.collect.ImmutableMap;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class InitializerTest extends AbstractMongoConnectionTest {

    @Test
    public void testInitializerMongo() throws CommitFailedException {
        NodeBuilder builder = mk.getNodeStore().getRoot().builder();
        new InitialContent().initialize(builder);

        SecurityProviderImpl provider = new SecurityProviderImpl(
                ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME,
                        ConfigurationParameters.of(ImmutableMap.of("anonymousId", "anonymous",
                                "adminId", "admin",
                                "usersPath", "/home/users",
                                "groupsPath", "/home/groups",
                                "defaultDepth", "1")))));
        provider.getConfiguration(UserConfiguration.class).getWorkspaceInitializer().initialize(
                builder, "default");
        builder.getNodeState();
    }

    @Test
    public void testInitializerSegment() throws CommitFailedException {
        NodeStore store = new SegmentNodeStore(new MemoryStore());

        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);

        SecurityProviderImpl provider = new SecurityProviderImpl(
                ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME,
                        ConfigurationParameters.of(ImmutableMap.of("anonymousId", "anonymous",
                                "adminId", "admin",
                                "usersPath", "/home/users",
                                "groupsPath", "/home/groups",
                                "defaultDepth", "1")))));
        provider.getConfiguration(UserConfiguration.class).getWorkspaceInitializer().initialize(
                builder, "default");
        builder.getNodeState();
    }

    @Test
    public void testInitializerMongoAfterInitialContent() throws CommitFailedException {
        NodeBuilder builder = mk.getNodeStore().getRoot().builder();
        // add initial content in separate merge
        new InitialContent().initialize(builder);
        mk.getNodeStore().merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        SecurityProviderImpl provider = new SecurityProviderImpl(
                ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME,
                        ConfigurationParameters.of(ImmutableMap.of("anonymousId", "anonymous",
                                "adminId", "admin",
                                "usersPath", "/home/users",
                                "groupsPath", "/home/groups",
                                "defaultDepth", "1")))));
        provider.getConfiguration(UserConfiguration.class).getWorkspaceInitializer().initialize(
                builder, "default");
        builder.getNodeState();
    }
}
