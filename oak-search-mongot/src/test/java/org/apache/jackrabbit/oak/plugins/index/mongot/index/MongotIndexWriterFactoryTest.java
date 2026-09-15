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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongotIndexWriterFactoryTest {

    @Test
    public void realTimeModeFollowsElasticPrecedenceAndExcludesAsyncIndexes() {
        NodeBuilder configured = EmptyNodeState.EMPTY_NODE.builder();
        configured.setProperty("sync-mode", "rt");
        MongotIndexDefinition definition = definition(configured);

        assertTrue(MongotIndexWriterFactory.isRealTime(definition, CommitInfo.EMPTY));
        assertTrue(MongotIndexWriterFactory.isRealTime(definition,
                commitInfo(Map.of("sync-mode", "rt"))));
        assertFalse(MongotIndexWriterFactory.isRealTime(definition,
                commitInfo(Map.of("sync-mode", "eventual"))));

        configured.setProperty(ASYNC_PROPERTY_NAME, "async");
        assertFalse(MongotIndexWriterFactory.isRealTime(definition(configured),
                commitInfo(Map.of("sync-mode", "rt"))));
    }

    private static MongotIndexDefinition definition(NodeBuilder builder) {
        return new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.getNodeState(), "/oak:index/test");
    }

    private static CommitInfo commitInfo(Map<String, Object> info) {
        return new CommitInfo("test", "test", info);
    }
}
