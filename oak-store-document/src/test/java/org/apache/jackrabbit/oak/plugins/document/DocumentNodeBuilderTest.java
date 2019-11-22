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

import java.util.Arrays;
import java.util.Collection;

import org.apache.jackrabbit.oak.fixture.DocumentMemoryFixture;
import org.apache.jackrabbit.oak.fixture.MemoryFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.UPDATE_LIMIT;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DocumentNodeBuilderTest {

    private final NodeStoreFixture fixture;

    private NodeStore ns;

    public DocumentNodeBuilderTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> fixtures() {
        return Arrays.asList(new Object[][]{
                {new MemoryFixture()},
                {new DocumentMemoryFixture()}
        });
    }

    @Before
    public void before() {
        ns = fixture.createNodeStore();
    }

    @After
    public void after() {
        fixture.dispose(ns);
    }

    @Test
    public void getBaseState() {
        NodeState state = ns.getRoot();
        NodeBuilder builder = state.builder();
        assertStateEquals(state, builder.getBaseState());
    }

    @Test
    public void getBaseStateFromModifiedBuilder() {
        NodeState state = ns.getRoot();
        NodeBuilder builder = state.builder();
        builder.child("foo");
        assertStateEquals(state, builder.getBaseState());
    }

    @Test
    public void getBaseStateFromModifiedBuilderBranched() {
        NodeState state = ns.getRoot();
        NodeBuilder builder = state.builder();
        for (int i = 0; i < UPDATE_LIMIT; i++) {
            builder.child("c-" + i).setProperty("p", "v");
        }
        assertStateEquals(state, builder.getBaseState());
    }

    @Test
    public void getBaseStateFromBuilderFromStateFromModifiedBuilder() {
        NodeState state = ns.getRoot();
        NodeBuilder builder = state.builder();
        builder.child("foo");
        NodeState modifiedState = builder.getNodeState();
        assertStateEquals(modifiedState, modifiedState.builder().getBaseState());
    }

    private void assertStateEquals(NodeState one, NodeState two) {
        String changesOnTwo = JsopDiff.diffToJsop(one, two);
        assertTrue("Node states are not equal: " + changesOnTwo,
                EqualsDiff.equals(one, two));
    }
}
