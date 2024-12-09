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
package org.apache.jackrabbit.oak.spi.mount;

import java.util.Collection;
import java.util.Collections;

import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.stream.Collectors.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimpleMountInfoProviderTest {
    @Test
    public void defaultMount() throws Exception {
        MountInfoProvider mip = new SimpleMountInfoProvider(Collections.<Mount>emptyList());

        assertNotNull(mip.getMountByPath("/a"));
        assertTrue(mip.getMountByPath("/a").isDefault());
        assertFalse(mip.hasNonDefaultMounts());
    }

    @Test
    public void basicMounting() throws Exception {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a", "/b")
                .mount("bar", "/x", "/y")
                .build();

        assertEquals("foo", mip.getMountByPath("/a").getName());
        assertEquals("foo", mip.getMountByPath("/a/x").getName());
        assertEquals("bar", mip.getMountByPath("/x").getName());
        assertTrue(mip.getMountByPath("/z").isDefault());
        assertTrue(mip.hasNonDefaultMounts());
    }

    @Test
    public void nonDefaultMounts() throws Exception{
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a", "/b")
                .mount("bar", "/x", "/y")
                .build();

        Collection<Mount> mounts = mip.getNonDefaultMounts();
        assertEquals(2, mounts.size());
        assertFalse(mounts.contains(mip.getDefaultMount()));

        assertNotNull(mip.getMountByName("foo"));
        assertNotNull(mip.getMountByName("bar"));
        assertNull(mip.getMountByName("boom"));
    }

    @Test
    public void readOnlyMounting() throws Exception{
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a", "/b")
                .readOnlyMount("bar", "/x", "/y")
                .build();

        assertTrue(mip.getMountByName("bar").isReadOnly());
        assertFalse(mip.getMountByName("foo").isReadOnly());
    }
    
    @Test
    public void mountsPlacedUnder() {
        
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("first", "/a")
                .mount("second", "/e", "/b/d")
                .mount("third", "/f", "/b/c")
                .build();

        assertEquals(Set.of("first", "second", "third"), mountNames(mip.getMountsPlacedUnder("/")));
        assertEquals(Set.of("second", "third"), mountNames(mip.getMountsPlacedUnder("/b")));
    }

    @Test
    public void mountsPlacedUnderPathsAtDifferentDepth() {
        MountInfoProvider mip = Mounts.newBuilder()
            .mount("xxx", "/foo/xxx", "/a/b/c")
            .mount("yyy", "/yyy", "/foo/yyy", "/a/b/d")
            .mount("zzz", "/z/zz")
            .build();

        assertEquals(Set.of("xxx", "yyy", "zzz"), mountNames(mip.getMountsPlacedUnder("/")));
        assertEquals(Set.of("xxx", "yyy"), mountNames(mip.getMountsPlacedUnder("/a")));
        assertEquals(Set.of("xxx", "yyy"), mountNames(mip.getMountsPlacedUnder("/a/b")));
        assertEquals(Set.of("xxx", "yyy"), mountNames(mip.getMountsPlacedUnder("/foo")));
        assertEquals(Set.of("zzz"), mountNames(mip.getMountsPlacedUnder("/z")));
    }

    @Test
    public void mountsPlacedDirectlyUnderPathsAtDifferentDepth() {
        MountInfoProvider mip = Mounts.newBuilder()
            .mount("xxx", "/xxx", "/foo/xxx", "/a/b/c")
            .mount("yyy", "/foo/yyy", "/a/b/d")
            .mount("zzz", "/z/zz")
            .build();

        assertEquals(Set.of("xxx"), mountNames(mip.getMountsPlacedDirectlyUnder("/")));
        assertEquals(Set.of("xxx", "yyy"), mountNames(mip.getMountsPlacedDirectlyUnder("/foo")));
        assertEquals(Set.of(), mountNames(mip.getMountsPlacedDirectlyUnder("/a")));
        assertEquals(Set.of("xxx", "yyy"), mountNames(mip.getMountsPlacedDirectlyUnder("/a/b")));
        assertEquals(Set.of("zzz"), mountNames(mip.getMountsPlacedDirectlyUnder("/z")));
    }

    @NotNull
    private static Set<String> mountNames(Collection<Mount> mip) {
        return mip.stream().map(Mount::getName).collect(toSet());
    }
}