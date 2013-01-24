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
package org.apache.jackrabbit.mongomk.impl.model.tree;

import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.NodeDiffHandler;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.NodeStateDiff;
import org.apache.jackrabbit.mk.model.tree.NodeStore;
import org.apache.jackrabbit.mongomk.api.model.Node;

/**
 * This dummy NodeStore implementation is needed in order to be able to reuse
 * Oak's DiffBuilder in MongoMK.
 */
public class SimpleMongoNodeStore implements NodeStore {

    @Override
    public NodeState getRoot() {
        return null;
    }

    @Override
    public void compare(final NodeState before, final NodeState after,
            final NodeStateDiff diff) {

        Node beforeNode = ((MongoNodeState)before).unwrap();
        Node afterNode = ((MongoNodeState)after).unwrap();

        beforeNode.diff(afterNode, new NodeDiffHandler() {
            @Override
            public void propAdded(String propName, String value) {
                diff.propertyAdded(after.getProperty(propName));
            }

            @Override
            public void propChanged(String propName, String oldValue,
                    String newValue) {
                diff.propertyChanged(before.getProperty(propName),
                        after.getProperty(propName));
            }

            @Override
            public void propDeleted(String propName, String value) {
                diff.propertyDeleted(before.getProperty(propName));
            }

            @Override
            public void childNodeAdded(ChildNodeEntry added) {
                String name = added.getName();
                diff.childNodeAdded(name, after.getChildNode(name));
            }

            @Override
            public void childNodeDeleted(ChildNodeEntry deleted) {
                String name = deleted.getName();
                diff.childNodeDeleted(name, before.getChildNode(name));
            }

            @Override
            public void childNodeChanged(ChildNodeEntry changed, Id newId) {
                String name = changed.getName();
                diff.childNodeChanged(name, before.getChildNode(name),
                        after.getChildNode(name));
            }
        });
    }
}