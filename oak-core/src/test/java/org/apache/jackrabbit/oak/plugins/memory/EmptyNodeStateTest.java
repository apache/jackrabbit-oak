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
package org.apache.jackrabbit.oak.plugins.memory;

import java.util.HashMap;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmptyNodeStateTest {

    @Test
    public void emptyEqualsMissing() {
        NodeState empty = EmptyNodeState.EMPTY_NODE;
        NodeState missing = new ModifiedNodeState(
                EmptyNodeState.MISSING_NODE,
                new HashMap<String, PropertyState>(),
                new HashMap<String, MutableNodeState>());
        assertTrue(empty.exists());
        assertFalse(missing.exists());
        assertFalse(missing.equals(empty));
        assertFalse(empty.equals(missing));
    }

    @Test
    public void missingEqualsModified() {
        NodeState empty = new ModifiedNodeState(
                EmptyNodeState.EMPTY_NODE,
                new HashMap<String, PropertyState>(),
                new HashMap<String, MutableNodeState>());
        NodeState missing = EmptyNodeState.MISSING_NODE;
        assertTrue(empty.exists());
        assertFalse(missing.exists());
        assertFalse(missing.equals(empty));
        assertFalse(empty.equals(missing));
    }
}
