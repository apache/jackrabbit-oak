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
package org.apache.jackrabbit.oak.plugins.version;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.version.VersionConstants.NT_CONFIGURATION;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VersionStorageEditorTest {

    @Test
    public void isVersionStorageNode() {
        NodeState nodeState = mock(NodeState.class);
        when(nodeState.getName(anyString())).thenReturn(REP_VERSIONSTORAGE);
        assertTrue(VersionStorageEditor.isVersionStorageNode(nodeState));
    }

    @Test
    public void isVersionStorageNode2() {
        NodeState nodeState = mock(NodeState.class);
        when(nodeState.getName(anyString())).thenReturn(NT_CONFIGURATION);
        assertTrue(VersionStorageEditor.isVersionStorageNode(nodeState));
    }

    @Test
    public void isVersionStorageNodeWithNullType() {
        NodeState nodeState = mock(NodeState.class);
        when(nodeState.getName(anyString())).thenReturn(null);
        assertFalse(VersionStorageEditor.isVersionStorageNode(nodeState));
    }
}