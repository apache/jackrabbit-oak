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
package org.apache.jackrabbit.oak.plugins.nodetype;

import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * Validator implementation that is responsible for validating any modification
 * made to node type definitions. This includes:
 *
 * <ul>
 *     <li>validate new definitions</li>
 *     <li>detect collisions,</li>
 *     <li>prevent circular inheritance,</li>
 *     <li>reject modifications to definitions that render existing content invalid,</li>
 *     <li>prevent un-registration of built-in node types.</li>
 * </ul>
 */
class RegistrationValidator implements Validator {

    private final ReadOnlyNodeTypeManager beforeMgr;
    private final ReadOnlyNodeTypeManager afterMgr;

    private final ReadOnlyTree parentBefore;
    private final ReadOnlyTree parentAfter;

    RegistrationValidator(ReadOnlyNodeTypeManager beforeMgr, ReadOnlyNodeTypeManager afterMgr,
                          ReadOnlyTree parentBefore, ReadOnlyTree parentAfter) {
        this.beforeMgr = beforeMgr;
        this.afterMgr = afterMgr;
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // TODO
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        // TODO
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // TODO
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        // TODO
        return null;
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // TODO
        return null;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        NodeUtil nodeBefore = new NodeUtil(new ReadOnlyTree(parentBefore, name, before));
        if (nodeBefore.hasPrimaryNodeTypeName(JcrConstants.NT_NODETYPE)) {
            if (isBuiltInNodeType(name)) {
                throw new CommitFailedException(new ConstraintViolationException("Attempt to unregister a built-in node type"));
            }
        }
        // TODO
        return null;
    }

    //------------------------------------------------------------< private >---

    private static boolean isBuiltInNodeType(String name) {
        // cheap way to determine if a given node type should be considered built-in
        String prefix = Text.getNamespacePrefix(name);
        return NamespaceConstants.RESERVED_PREFIXES.contains(prefix);
    }
}