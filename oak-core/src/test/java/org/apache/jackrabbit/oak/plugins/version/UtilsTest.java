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
package org.apache.jackrabbit.oak.plugins.version;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void frozenNodeReferenceable() {
        assertTrue(Utils.isFrozenNodeReferenceable(InitialContentHelper.INITIAL_CONTENT_FROZEN_NODE_REFERENCEABLE));
    }

    @Test
    public void frozenNodeNotReferenceable() {
        // OAK-9134 : the backport from trunk to 1.22 branch changed the default of
        // DEFAULT_REFERENCEABLE_FROZEN_NODE = true
        // to keep backwards compatibility in that branch
        // therefore this test is the opposite of what it is in trunk
        assertTrue(Utils.isFrozenNodeReferenceable(InitialContentHelper.INITIAL_CONTENT));
    }

    @Test
    public void frozenNodeDefinitionMissing() {
        // assume empty repository on recent Oak without referenceable nt:frozenNode
        assertFalse(Utils.isFrozenNodeReferenceable(EmptyNodeState.EMPTY_NODE));
    }
}
