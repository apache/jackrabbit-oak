/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.composite.checks.NodeStoreChecksService;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;

public class CompositeNodeStoreServiceTest {

	@Rule
	public final OsgiContext ctx = new OsgiContext();

	@Test
	public void bootstrapMultiMount() throws CommitFailedException {

		// Create node stores
		MemoryNodeStore nodeStoreLibs = new MemoryNodeStore();
		MemoryNodeStore nodeStoreApps = new MemoryNodeStore();
		MemoryNodeStore globalStore = new MemoryNodeStore();
		NodeBuilder globalRoot = globalStore.getRoot().builder();
		NodeBuilder global = globalRoot.child("content");
		global.child("content1");
		global.child("content2");
		globalStore.merge(globalRoot, EmptyHook.INSTANCE, CommitInfo.EMPTY);

		// Initialise node stores
		NodeBuilder appsRoot = nodeStoreApps.getRoot().builder();
		NodeBuilder apps = appsRoot.child("apps");
		apps.child("app1");
		apps.child("app2");
		nodeStoreApps.merge(appsRoot, EmptyHook.INSTANCE, CommitInfo.EMPTY);

		NodeBuilder libsRoot = nodeStoreLibs.getRoot().builder();
		NodeBuilder libs = libsRoot.child("libs");
		libs.child("lib1");
		libs.child("lib2");
		nodeStoreLibs.merge(libsRoot, EmptyHook.INSTANCE, CommitInfo.EMPTY);

		// Define read-only mounts
		registerActivateMountInfoConfig("libs", ImmutableList.of("/libs"));
		registerActivateMountInfoConfig("apps", ImmutableList.of("/apps"));

        registerMountInfoProviderService("libs", "apps");
        
		// Register node stores
		ctx.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(globalStore), Map.of("role", "composite-global", "registerDescriptors", Boolean.TRUE));
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(nodeStoreLibs), Map.of("role", "composite-mount-libs"));
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(nodeStoreApps), Map.of("role", "composite-mount-apps"));
		ctx.registerInjectActivateService(new NodeStoreChecksService());

		ctx.registerInjectActivateService(new CompositeNodeStoreService());


		NodeStore nodeStore = ctx.getService(NodeStore.class);
		assertNotNull(nodeStore.getRoot().getChildNode("content").getChildNode("content1"));
		assertNotNull(nodeStore.getRoot().getChildNode("apps").getChildNode("app1"));
		assertNotNull(nodeStore.getRoot().getChildNode("libs").getChildNode("lib1"));
	}

	/**
	 *  Verifies that a minimally-configured <tt>CompositeNodeStore</tt> can be registered successfully
	 */
	@Test
	public void bootstrap() {
		
		MemoryNodeStore mount = new MemoryNodeStore();
		MemoryNodeStore global = new MemoryNodeStore();
		
		MountInfoProvider mip = Mounts.newBuilder().readOnlyMount("libs", "/libs", "/apps").build();
		
		ctx.registerService(MountInfoProvider.class, mip);
		ctx.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(global), Map.of("role", "composite-global", "registerDescriptors", Boolean.TRUE));
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(mount), Map.of("role", "composite-mount-libs"));
		ctx.registerInjectActivateService(new NodeStoreChecksService());
		
		ctx.registerInjectActivateService(new CompositeNodeStoreService());


		assertThat("No NodeStore registered", ctx.getService(NodeStore.class), notNullValue());
	}
	
	/**
	 * Verifies that a missing mount will result in the node store not being registered
	 */
	@Test
	public void bootstrap_missingMount() {

		MemoryNodeStore mount = new MemoryNodeStore();
		MemoryNodeStore global = new MemoryNodeStore();
		
		MountInfoProvider mip = Mounts.newBuilder().readOnlyMount("libs", "/libs", "/apps").readOnlyMount("missing", "/missing").build();
		
		ctx.registerService(MountInfoProvider.class, mip);
		ctx.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(global), Map.of("role", "composite-global", "registerDescriptors", Boolean.TRUE));
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(mount), Map.of("role", "composite-mount-libs"));
		ctx.registerInjectActivateService(new NodeStoreChecksService());
		
		ctx.registerInjectActivateService(new CompositeNodeStoreService());
		
		assertThat("NodeStore registered, but it should not have been", ctx.getService(NodeStore.class), nullValue());
	}
	
	/**
	 *  Verifies that a missing global mount will result in the node store not being registered
	 */
	@Test
	public void bootstrap_missingGlobalMount() {
		
		MemoryNodeStore mount = new MemoryNodeStore();
		
		MountInfoProvider mip = Mounts.newBuilder().readOnlyMount("libs", "/libs", "/apps").build();
		
		ctx.registerService(MountInfoProvider.class, mip);
		ctx.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(mount), Map.of("role", "composite-mount-libs"));
		ctx.registerInjectActivateService(new NodeStoreChecksService());
		
		ctx.registerInjectActivateService(new CompositeNodeStoreService());
		
		assertThat("NodeStore registered, but it should not have been", ctx.getService(NodeStore.class), nullValue());
	}	

	private static class SimpleNodeStoreProvider implements NodeStoreProvider {

		private final NodeStore nodeStore;

		private SimpleNodeStoreProvider(NodeStore nodeStore) {
			this.nodeStore = nodeStore;
		}

		@Override
		public NodeStore getNodeStore() {
			return nodeStore;
		}

	}

	private void registerActivateMountInfoConfig(String mountName, List<String> mountedPaths) {
        MountInfoConfig mountInfoConfig = new MountInfoConfig();
        ctx.registerService(mountInfoConfig);
        mountInfoConfig.activate(ctx.bundleContext(), new MountInfoPropsBuilder()
            .withMountPaths(mountedPaths.toArray(new String[0]))
            .withMountName(mountName)
            .buildMountInfoProps());
    }

    private void registerMountInfoProviderService(String... expectedMounts) {
        MountInfoProviderService mountInfoProviderService = new MountInfoProviderService();
        ctx.registerInjectActivateService(mountInfoProviderService);
        mountInfoProviderService.activate(ctx.bundleContext(), new MountInfoPropsBuilder()
            .withExpectedMounts(expectedMounts)
            .buildProviderServiceProps());
    }
}
