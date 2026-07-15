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

import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class NodeUtils {

    private NodeUtils() {
        // Prevent instantiation.
    }

    static boolean hasChildNode(Iterable<? extends ChildNodeEntry> entries, String name) {
        return StreamSupport.stream(entries.spliterator(), false)
            .map(ChildNodeEntry::getName)
            .anyMatch(name::equals);
    }

    static NodeState getChildNode(Iterable<? extends ChildNodeEntry> entries, String name) {
        return StreamSupport.stream(entries.spliterator(), false)
            .filter(e -> e.getName().equals(name))
            .map(ChildNodeEntry::getNodeState)
            .findFirst()
            .orElse(EmptyNodeState.MISSING_NODE);
    }

}
