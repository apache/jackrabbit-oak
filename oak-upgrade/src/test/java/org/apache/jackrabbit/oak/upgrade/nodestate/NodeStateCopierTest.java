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
package org.apache.jackrabbit.oak.upgrade.nodestate;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.upgrade.util.NodeStateTestUtils.commit;
import static org.apache.jackrabbit.oak.upgrade.util.NodeStateTestUtils.create;
import static org.apache.jackrabbit.oak.upgrade.util.NodeStateTestUtils.createNodeStoreWithContent;
import static org.apache.jackrabbit.oak.upgrade.util.NodeStateTestUtils.expectDifference;

public class NodeStateCopierTest {

    private final PropertyState primaryType =
            createProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

    @Test
    public void shouldMergeIdenticalContent() throws CommitFailedException {
        final NodeStore source = createPrefilledNodeStore();
        final NodeStore target = createPrefilledNodeStore();

        final NodeState before = target.getRoot();
        NodeStateCopier.copyNodeStore(source, target);
        final NodeState after = target.getRoot();

        expectDifference().strict().verify(before, after);
        expectDifference().strict().verify(source.getRoot(), after);
    }

    private NodeStore createPrefilledNodeStore() throws CommitFailedException {
        final NodeStore store = createNodeStoreWithContent();
        final NodeBuilder builder = store.getRoot().builder();
        create(builder, "/excluded");
        create(builder, "/a", primaryType, createProperty("name", "a"));
        create(builder, "/a/b", primaryType, createProperty("name", "b"));
        create(builder, "/a/b/excluded");
        create(builder, "/a/b/c", primaryType, createProperty("name", "c"));
        create(builder, "/a/b/c/d", primaryType);
        create(builder, "/a/b/c/e", primaryType);
        create(builder, "/a/b/c/f", primaryType);
        commit(store, builder);
        return store;
    }

    @Test
    public void shouldRespectMergePaths() throws CommitFailedException {
        final NodeStore source = createNodeStoreWithContent("/content/foo/en", "/content/bar/en");
        final NodeStore target = createNodeStoreWithContent("/content/foo/de");

        final NodeBuilder builder = target.getRoot().builder();
        NodeStateCopier.copyNodeState(source.getRoot(), builder, "/", of("/content"));
        commit(target, builder);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo/de")
                .childNodeChanged("/content", "/content/foo")
                .verify(source.getRoot(), after);
    }


    @Test
    public void shouldDeleteExistingNodes() throws CommitFailedException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        final NodeBuilder builder = before.builder();
        NodeStateCopier.copyNodeState(source.getRoot(), builder, "/", ImmutableSet.<String>of());
        commit(target, builder);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .childNodeDeleted("/content/bar")
                .verify(before, after);
    }

    @Test
    public void shouldDeleteExistingPropertyIfMissingInSource() throws CommitFailedException {
        final NodeStore source = createNodeStoreWithContent("/a");
        final NodeStore target = createNodeStoreWithContent();
        NodeBuilder builder = target.getRoot().builder();
        create(builder, "/a", primaryType);
        commit(target, builder);

        final NodeState before = target.getRoot();
        builder = before.builder();
        NodeStateCopier.copyNodeState(source.getRoot(), builder, "/", ImmutableSet.<String>of());
        commit(target, builder);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .propertyDeleted("/a/jcr:primaryType")
                .childNodeChanged("/a")
                .verify(before, after);
    }

    @Test
    public void shouldNotDeleteExistingNodesIfMerged() throws CommitFailedException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        final NodeBuilder builder = before.builder();
        NodeStateCopier.copyNodeState(source.getRoot(), builder, "/", of("/content/bar"));
        commit(target, builder);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .verify(before, after);
    }

    @Test
    public void shouldNotDeleteExistingNodesIfDescendantsOfMerged() throws CommitFailedException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        final NodeBuilder builder = before.builder();
        NodeStateCopier.copyNodeState(source.getRoot(), builder, "/", of("/content"));
        commit(target, builder);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .verify(before, after);
    }

    @Test
    public void shouldIgnoreNonMatchingMergePaths() throws CommitFailedException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        final NodeBuilder builder = before.builder();
        NodeStateCopier.copyNodeState(source.getRoot(), builder, "/", of("/con"));
        commit(target, builder);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .childNodeDeleted("/content/bar")
                .verify(before, after);
    }

}
