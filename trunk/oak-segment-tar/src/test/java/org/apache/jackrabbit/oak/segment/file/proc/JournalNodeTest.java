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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class JournalNodeTest {

    @Test
    public void shouldExposeCommitHandle() {
        Backend backend = mock(Backend.class);
        when(backend.commitExists("h")).thenReturn(true);

        NodeState n  = new JournalNode(backend);

        assertTrue(n.hasChildNode("h"));
        assertTrue(n.getChildNode("h").exists());
    }

    @Test
    public void shouldNotExposeNonExistingHandle() {
        NodeState n = new JournalNode(mock(Backend.class));

        assertFalse(n.hasChildNode("h"));
        assertFalse(n.getChildNode("h").exists());
    }

    @Test
    public void shouldExposeAllCommitHandles() {
        Set<String> names = Sets.newHashSet("h1", "h2", "h3");

        Backend backend = mock(Backend.class);
        when(backend.getCommitHandles()).thenReturn(names);

        assertEquals(names, Sets.newHashSet(new JournalNode(backend).getChildNodeNames()));
    }

}
