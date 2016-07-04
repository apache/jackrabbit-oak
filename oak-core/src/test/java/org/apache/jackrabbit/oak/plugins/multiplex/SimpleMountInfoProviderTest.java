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

package org.apache.jackrabbit.oak.plugins.multiplex;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimpleMountInfoProviderTest {
    @Test
    public void defaultMount() throws Exception {
        MountInfoProvider mip = new SimpleMountInfoProvider(Collections.<MountInfo>emptyList());

        assertNotNull(mip.getMountInfo("/a"));
        assertTrue(mip.getMountInfo("/a").isDefault());
    }

    @Test
    public void basicMounting() throws Exception {
        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("foo", "/a", "/b")
                .mount("bar", "/x", "/y")
                .build();

        assertEquals("foo", mip.getMountInfo("/a").getName());
        assertEquals("foo", mip.getMountInfo("/a/x").getName());
        assertEquals("bar", mip.getMountInfo("/x").getName());
        assertTrue(mip.getMountInfo("/z").isDefault());
    }

    @Test
    public void nonDefaultMounts() throws Exception{
        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("foo", "/a", "/b")
                .mount("bar", "/x", "/y")
                .build();

        Collection<Mount> mounts = mip.getNonDefaultMounts();
        assertEquals(2, mounts.size());
        assertFalse(mounts.contains(Mount.DEFAULT));

        assertNotNull(mip.getMount("foo"));
        assertNotNull(mip.getMount("bar"));
        assertNull(mip.getMount("boom"));
    }

    @Test
    public void readOnlyMounting() throws Exception{
        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("foo", "/a", "/b")
                .readOnlyMount("bar", "/x", "/y")
                .build();

        assertTrue(mip.getMount("bar").isReadOnly());
        assertFalse(mip.getMount("foo").isReadOnly());
    }

}