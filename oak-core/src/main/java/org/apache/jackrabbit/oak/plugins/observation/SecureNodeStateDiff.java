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

package org.apache.jackrabbit.oak.plugins.observation;

import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.RecursingNodeStateDiff;

public class SecureNodeStateDiff extends SecurableNodeStateDiff {
    private SecureNodeStateDiff(RecursingNodeStateDiff diff, Tree before, Tree after) {
        super(diff, before, after);
    }

    private SecureNodeStateDiff(SecurableNodeStateDiff parent, Tree beforeParent, Tree afterParent, String name) {
        super(parent, beforeParent, afterParent, name);
    }

    public static void compare(RecursingNodeStateDiff diff, ImmutableTree before, ImmutableTree after) {
        SecureNodeStateDiff secureDiff = new SecureNodeStateDiff(diff, before, after);
        after.getNodeState().compareAgainstBaseState(before.getNodeState(), secureDiff);
    }

    @Override
    protected SecurableNodeStateDiff create(SecurableNodeStateDiff parent,
            String name, NodeState before, NodeState after) {

        return isHidden(name) ? null : new SecureNodeStateDiff(parent, beforeTree, afterTree, name);
    }

    @Override
    protected boolean canRead(Tree beforeParent, PropertyState before, Tree afterParent, PropertyState after) {
        // TODO implement canRead
        return true;
    }

    @Override
    protected boolean canRead(Tree before, Tree after) {
        // TODO implement canRead
        return true;
    }

    @Override
    protected NodeState secureBefore(String name, NodeState nodeState) {
        // TODO implement secureBefore
        return nodeState;
    }

    @Override
    protected NodeState secureAfter(String name, NodeState nodeState) {
        // TODO implement secureAfter
        return nodeState;
    }

}
