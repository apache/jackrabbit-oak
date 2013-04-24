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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

/**
 * Test for OAK-695.
 */
public class TypeEditorTest {

    @Test
    public void ignoreHidden() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = new InitialContent().initialize(EMPTY_NODE);
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();
        builder.child(":hidden");
        NodeState after = builder.getNodeState();
        hook.processCommit(before, after);

        before = after;
        builder.child(":hidden").setProperty("prop", "value");
        after = builder.getNodeState();
        hook.processCommit(before, after);

        before = after;
        builder.removeChildNode(":hidden");
        after = builder.getNodeState();
        hook.processCommit(before, after);
    }

}
