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
package org.apache.jackrabbit.mk.model.tree;

import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.NodeStateDiff;
import org.apache.jackrabbit.mk.model.tree.NodeStore;
import org.apache.jackrabbit.oak.commons.PathUtils;

import java.util.Stack;

/**
 *
 */
public abstract class TraversingNodeDiffHandler implements NodeStateDiff {

    private final NodeStore store;

    protected Stack<String> paths = new Stack<String>();

    public TraversingNodeDiffHandler(NodeStore store) {
        this.store = store;
    }

    public void start(NodeState before, NodeState after, String path) {
        paths.clear();
        paths.push(path);
        store.compare(before, after, this);
    }

    protected String getCurrentPath() {
        return paths.peek();
    }

    @Override
    public void childNodeChanged(
            String name, NodeState before, NodeState after) {
        paths.push(PathUtils.concat(getCurrentPath(), name));
        store.compare(before, after, this);
        paths.pop();
    }

}
