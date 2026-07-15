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

package org.apache.jackrabbit.oak.segment.file.proc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Commit;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class CommitNodeTest {

    private Commit mockCommit() {
        Commit commit = mock(Commit.class);
        when(commit.getRevision()).thenReturn("");
        return commit;
    }

    @Test
    public void shouldHaveTimestampProperty() {
        Commit commit = mockCommit();
        when(commit.getTimestamp()).thenReturn(1L);

        Proc.Backend backend = mock(Proc.Backend.class);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        PropertyState property = new CommitNode(backend, "h").getProperty("timestamp");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1L, property.getValue(Type.LONG).longValue());
    }

    @Test
    public void shouldExposeRoot() {
        Commit commit = mockCommit();
        when(commit.getRoot()).thenReturn(Optional.of(EmptyNodeState.EMPTY_NODE));

        Proc.Backend backend = mock(Proc.Backend.class);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        assertTrue(new CommitNode(backend, "h").hasChildNode("root"));
    }

    @Test
    public void shouldHaveRootChildNode() {
        NodeState root = EmptyNodeState.EMPTY_NODE;

        Commit commit = mock(Commit.class);
        when(commit.getRoot()).thenReturn(Optional.of(root));

        Proc.Backend backend = mock(Proc.Backend.class);
        when(backend.getCommit("h")).thenReturn(Optional.of(commit));

        assertSame(root, new CommitNode(backend, "h").getChildNode("root"));
    }

}
