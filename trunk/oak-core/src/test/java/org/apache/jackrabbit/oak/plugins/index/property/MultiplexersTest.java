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

package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.Multiplexers.getIndexNodeName;
import static org.apache.jackrabbit.oak.plugins.index.property.Multiplexers.getNodeForMount;
import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.junit.Before;
import org.junit.Test;

public class MultiplexersTest {

	private MountInfoProvider mip;

	@Before
	public void setUp() {
        mip = Mounts.defaultMountInfoProvider();
	}
	
    @Test
    public void defaultSetup() throws Exception {
		assertEquals(
                INDEX_CONTENT_NODE_NAME,
                getIndexNodeName(mip, "/foo",
                        INDEX_CONTENT_NODE_NAME));
        assertEquals(INDEX_CONTENT_NODE_NAME,
                getNodeForMount(mip.getDefaultMount(), INDEX_CONTENT_NODE_NAME));
    }

    @Test
    public void customNodeName() throws Exception {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a", "/b").build();

        Mount m = mip.getMountByName("foo");

        assertEquals(":index",
                getIndexNodeName(mip, "/foo", INDEX_CONTENT_NODE_NAME));
        assertEquals(":index",
                getNodeForMount(mip.getDefaultMount(), INDEX_CONTENT_NODE_NAME));

        assertEquals(":" + m.getPathFragmentName() + "-index",
                getIndexNodeName(mip, "/a", INDEX_CONTENT_NODE_NAME));
        assertEquals(":" + m.getPathFragmentName() + "-index",
                getNodeForMount(m, INDEX_CONTENT_NODE_NAME));
    }
}