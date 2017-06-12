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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class CrossMountReferenceValidatorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private EditorHook hook;

    @Before
    public void initializeHook() {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a")
                .build();
        hook = new EditorHook(new CompositeEditorProvider(
                new IndexUpdateProvider(new CompositeIndexEditorProvider(
                        new PropertyIndexEditorProvider().with(mip),
                        new ReferenceEditorProvider().with(mip))),
                        new CrossMountReferenceValidatorProvider().with(mip).withFailOnDetection(true)));
    }

    @Test
    public void globalToPrivateReference() throws Exception{
        NodeState root = INITIAL_CONTENT;

        NodeBuilder builder = root.builder();
        NodeState before = builder.getNodeState();

        builder.child("a").setProperty(createProperty(JCR_UUID, "u1", Type.STRING));
        builder.child("b").setProperty(createProperty("foo", "u1", Type.REFERENCE));

        NodeState after = builder.getNodeState();

        thrown.expect(CommitFailedException.class);
        thrown.expectMessage("OakIntegrity0001: Unable to reference the node [/a] from node [/b]. Referencing across the mounts is not allowed.");
        hook.processCommit(before, after, CommitInfo.EMPTY);
    }

    @Test
    public void privateToGlobalReference() throws Exception{
        NodeState root = INITIAL_CONTENT;

        NodeBuilder builder = root.builder();
        NodeState before = builder.getNodeState();

        builder.child("a").setProperty(createProperty("foo", "u1", Type.REFERENCE));
        builder.child("b").setProperty(createProperty(JCR_UUID, "u1", Type.STRING));

        NodeState after = builder.getNodeState();

        thrown.expect(CommitFailedException.class);
        thrown.expectMessage("OakIntegrity0001: Unable to reference the node [/b] from node [/a]. Referencing across the mounts is not allowed.");
        hook.processCommit(before, after, CommitInfo.EMPTY);
    }
}
