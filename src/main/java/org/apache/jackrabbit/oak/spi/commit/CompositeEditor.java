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
package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.List;

/**
 * Composite commit editor. Maintains a list of component editors and takes
 * care of calling them in proper sequence in the
 * {@link #editCommit(NodeStore, NodeState, NodeState)} method.
 */
public class CompositeEditor implements CommitEditor {

    private final List<CommitEditor> editors;

    public CompositeEditor(List<CommitEditor> editors) {
        this.editors = editors;
    }

    @Override
    public NodeState editCommit(
            NodeStore store, NodeState before, NodeState after)
            throws CommitFailedException {

        NodeState oldState = before;
        NodeState newState = after;
        for (CommitEditor editor : editors) {
            NodeState newOldState = newState;
            newState = editor.editCommit(store, oldState, newState);
            oldState = newOldState;
        }

        return newState;
    }

}
