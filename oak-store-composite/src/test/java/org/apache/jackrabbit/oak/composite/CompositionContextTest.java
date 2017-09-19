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

import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompositionContextTest {

    @Test
    public void testShouldBeComposite() {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("libs", true,
                        Arrays.asList(
                                "/oak:index/*$"
                        ),
                        Arrays.asList(
                                "/apps",
                                "/libs",
                                "/jcr:system/rep:permissionStore/oak:mount-libs-crx.default")
                        )
                .build();
        CompositionContext ctx = new CompositionContext(mip, null, Collections.emptyList(), CompositeNodeStoreMonitor.EMPTY_INSTANCE, CompositeNodeStoreMonitor.EMPTY_INSTANCE);

        assertTrue(ctx.shouldBeComposite("/"));
        assertTrue(ctx.shouldBeComposite("/oak:index"));
        assertTrue(ctx.shouldBeComposite("/oak:index/lucene"));
        assertTrue(ctx.shouldBeComposite("/jcr:system"));
        assertTrue(ctx.shouldBeComposite("/jcr:system/rep:permissionStore"));

        assertFalse(ctx.shouldBeComposite("/apps"));
        assertFalse(ctx.shouldBeComposite("/apps/acme"));
        assertFalse(ctx.shouldBeComposite("/libs"));
        assertFalse(ctx.shouldBeComposite("/libs/acme"));
        assertFalse(ctx.shouldBeComposite("/oak:index/lucene/:data"));
        assertFalse(ctx.shouldBeComposite("/oak:index/lucene/:oak:mount-libs-data"));
        assertFalse(ctx.shouldBeComposite("/jcr:system/rep:permissionStore/oak:mount-libs-crx.default"));
        assertFalse(ctx.shouldBeComposite("/jcr:system/rep:permissionStore/oak:mount-libs-crx.default/123"));
        assertFalse(ctx.shouldBeComposite("/jcr:system/rep:permissionStore/crx.default"));
        assertFalse(ctx.shouldBeComposite("/content"));
        assertFalse(ctx.shouldBeComposite("/jcr:system/rep:versionStorage"));
    }

}
