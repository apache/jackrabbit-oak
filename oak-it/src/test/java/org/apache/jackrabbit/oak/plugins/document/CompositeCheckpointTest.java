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

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.composite.CompositeNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class CompositeCheckpointTest {

    private DocumentNodeStore global;

    private NodeStore composite;

    @Before
    public void init() {
        MountInfoProvider mip = Mounts.newBuilder()
                .readOnlyMount("mnt", "/mnt")
                .build();

        global = new DocumentNodeStoreBuilder<>().build();

        composite = new CompositeNodeStore.Builder(mip, global)
                .addMount("mnt", new MemoryNodeStore())
                .build();
    }

    @After
    public void dispose() {
        global.dispose();
    }

    @Test
    public void createCheckpointOnGlobal() {
        Revision r = global.getHeadRevision().getRevision(global.getClusterId());
        assertNotNull(r);
        String result = global.getMBean().createCheckpoint(
                r.toString(), TimeUnit.HOURS.toMillis(1), false);
        String checkpoint = result.substring(result.indexOf("[") + 1, result.indexOf("]"));
        assertNotNull(composite.retrieve(checkpoint));
    }
}
