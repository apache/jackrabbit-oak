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

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.jackrabbit.oak.composite.checks.NodeStoreChecksService;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class CompositeNodeStoreServiceTest {

	@Rule
	public final OsgiContext ctx = new OsgiContext();

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
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(global), ImmutableMap.of("role", "composite-global", "registerDescriptors", Boolean.TRUE));
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(mount), ImmutableMap.of("role", "composite-mount-libs"));
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
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(global), ImmutableMap.of("role", "composite-global", "registerDescriptors", Boolean.TRUE));
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(mount), ImmutableMap.of("role", "composite-mount-libs"));
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
		ctx.registerService(NodeStoreProvider.class, new SimpleNodeStoreProvider(mount), ImmutableMap.of("role", "composite-mount-libs"));
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
}
