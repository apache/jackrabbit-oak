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

package org.apache.jackrabbit.oak.plugins.index.progress;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeCounterMBeanEstimatorTest {

    private final NodeStore store = new MemoryNodeStore();
    private NodeCounter counter = mock(NodeCounter.class);

    @Test
    public void estimateCount() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.child("idx-a");

        builder.child("idx-b").setProperty(PROP_INCLUDED_PATHS, asList("/content"), Type.STRINGS);
        builder.child("idx-b").setProperty(PROP_EXCLUDED_PATHS, asList("/content/old"), Type.STRINGS);

        builder.child("idx-c").setProperty(PROP_INCLUDED_PATHS, asList("/libs"), Type.STRINGS);

        builder.child("idx-d").setProperty(PROP_EXCLUDED_PATHS, asList("/libs"), Type.STRINGS);


        builder.child("content").child("idx-e").setProperty(PROP_EXCLUDED_PATHS, asList("/content/old"), Type.STRINGS);
        builder.child("content").child("idx-f");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Map<String, Integer> counts = ImmutableMap.of(
                "/", 100,
                "/content", 50,
                "/content/old", 10,
                "/libs", 30
        );

        when(counter.getEstimatedNodeCount(anyString())).then((invocation -> {
            String path = (String) invocation.getArguments()[0];
            if (counts.containsKey(path)) {
                return counts.get(path);
            }
            return -1;
        }));

        NodeCountEstimator estimator = new NodeCounterMBeanEstimator(store, counter);

        assertEquals(100, estimator.getEstimatedNodeCount("/", of("/idx-a")));
        assertEquals(100, estimator.getEstimatedNodeCount("/", of("/idx-a", "/idx-b")));
        assertEquals(40, estimator.getEstimatedNodeCount("/", of("/idx-b")));
        assertEquals(70, estimator.getEstimatedNodeCount("/", of("/idx-b", "/idx-c")));

        assertEquals(50, estimator.getEstimatedNodeCount("/content", of("/content/idx-f")));
        assertEquals(40, estimator.getEstimatedNodeCount("/content", of("/content/idx-e")));
    }

}