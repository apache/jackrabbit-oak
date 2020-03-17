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

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.spi.mount.MountInfo;
import org.junit.Test;

import java.util.Collections;

public class MountInfoTest {

    @Test
    public void testIsMounted() throws Exception{
        MountInfo md = new MountInfo("foo", false, of("/x/y"), of("/a", "/b"));
        assertTrue(md.isMounted("/a"));
        assertTrue(md.isMounted("/b"));
        assertTrue(md.isMounted("/b/c/d"));
        assertTrue("dynamic mount path not recognized", md.isMounted("/x/y/oak:mount-foo/a"));
        assertTrue("dynamic mount path not recognized", md.isMounted("/x/y/z/oak:mount-foo/a"));
        assertFalse(md.isMounted("/x/y"));
        assertFalse(md.isMounted("/x/y/foo"));
        assertFalse(md.isMounted("/d/c/oak:mount-foo/a"));
    }

    @Test
    public void testIsUnder() {
        MountInfo md = new MountInfo("foo", false, Collections.<String>emptyList(), of("/apps", "/etc/config", "/content/my/site", "/var"));
        assertTrue(md.isUnder("/etc"));
        assertTrue(md.isUnder("/content"));
        assertTrue(md.isUnder("/content/my"));
        assertFalse(md.isUnder("/content/my/site"));
        assertFalse(md.isUnder("/libs"));
        assertFalse(md.isUnder("/tmp"));
    }

    @Test
    public void testIsDirectlyUnder() {
        MountInfo md = new MountInfo("foo", false, Collections.<String>emptyList(), of("/apps", "/etc/my/config", "/var"));
        assertFalse(md.isDirectlyUnder("/etc"));
        assertTrue(md.isDirectlyUnder("/etc/my"));
        assertFalse(md.isDirectlyUnder("/etc/my/config"));
        assertFalse(md.isDirectlyUnder("/libs"));
    }

    @Test
    public void testSupportFragment() {
        MountInfo md = new MountInfo("foo", false, of("/apps", "/libs/*/site", "/content/*$", "/var$"), Collections.emptyList());

        assertFalse(md.isSupportFragment("/"));
        assertTrue(md.isSupportFragment("/apps"));
        assertTrue(md.isSupportFragment("/apps/acme"));

        assertFalse(md.isSupportFragment("/libs"));
        assertFalse(md.isSupportFragment("/libs/acme"));
        assertTrue(md.isSupportFragment("/libs/acme/site"));
        assertTrue(md.isSupportFragment("/libs/acme/site/abc"));

        assertFalse(md.isSupportFragment("/content"));
        assertTrue(md.isSupportFragment("/content/site"));
        assertFalse(md.isSupportFragment("/content/site/abc"));

        assertTrue(md.isSupportFragment("/var"));
        assertFalse(md.isSupportFragment("/var/abc"));
    }

    @Test
    public void testSupportFragmentUnder() {
        MountInfo md = new MountInfo("foo", false, of("/apps", "/libs/*/site", "/content/*$", "/var$"), Collections.emptyList());

        assertTrue(md.isSupportFragmentUnder("/"));
        assertTrue(md.isSupportFragmentUnder("/apps"));
        assertTrue(md.isSupportFragmentUnder("/apps/acme"));

        assertTrue(md.isSupportFragmentUnder("/libs"));
        assertTrue(md.isSupportFragmentUnder("/libs/acme"));
        assertTrue(md.isSupportFragmentUnder("/libs/acme/site"));
        assertTrue(md.isSupportFragmentUnder("/libs/acme/site/abc"));

        assertTrue(md.isSupportFragmentUnder("/content"));
        assertTrue(md.isSupportFragmentUnder("/content/site"));
        assertFalse(md.isSupportFragmentUnder("/content/site/abc"));

        assertTrue(md.isSupportFragmentUnder("/var"));
        assertFalse(md.isSupportFragmentUnder("/var/abc"));
    }

    @Test
    public void testIsMountedWithFragments() {
        MountInfo md = new MountInfo("foo", false, of("/apps", "/libs/*/site", "/content/*$", "/var$"), Collections.emptyList());

        assertFalse(md.isMounted("/oak:mount-foo"));
        assertTrue(md.isMounted("/apps/oak:mount-foo"));
        assertTrue(md.isMounted("/apps/oak:mount-foo/xyz"));
        assertTrue(md.isMounted("/apps/acme/oak:mount-foo"));
        assertTrue(md.isMounted("/apps/acme/oak:mount-foo/xyz"));

        assertFalse(md.isMounted("/libs/oak:mount-foo"));
        assertFalse(md.isMounted("/libs/acme/oak:mount-foo"));
        assertTrue(md.isMounted("/libs/acme/site/oak:mount-foo"));
        assertTrue(md.isMounted("/libs/acme/site/oak:mount-foo/xyz"));
        assertTrue(md.isMounted("/libs/acme/site/abc/oak:mount-foo"));

        assertFalse(md.isMounted("/oak:mount-foo"));
        assertFalse(md.isMounted("/content/oak:mount-foo"));
        assertTrue(md.isMounted("/content/site/oak:mount-foo"));
        assertTrue(md.isMounted("/content/site/oak:mount-foo/xyz"));
        assertFalse(md.isMounted("/content/site/abc/oak:mount-foo"));
        assertFalse(md.isMounted("/content/site/abc/oak:mount-foo/xyz"));

        assertTrue(md.isMounted("/var/oak:mount-foo"));
        assertFalse(md.isMounted("/var/abc/oak:mount-foo"));
    }
}