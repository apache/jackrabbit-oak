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
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;

import javax.jcr.ItemNotFoundException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import java.util.Iterator;
import java.util.List;

public class NodeDelegate extends ItemDelegate {

    private final SessionContext sessionContext;
    private Tree tree;

    NodeDelegate(SessionContext sessionContext, Tree tree) {
        this.sessionContext = sessionContext;
        this.tree = tree;
    }

    NodeDelegate addNode(String relPath) throws RepositoryException {
        Tree parentState = getTree(PathUtils.getParentPath(relPath));
        if (parentState == null) {
            throw new PathNotFoundException(relPath);
        }

        String name = PathUtils.getName(relPath);
        parentState.addChild(name);
        return new NodeDelegate(sessionContext, parentState.getChild(name));
    }

    NodeDelegate getAncestor(int depth) throws RepositoryException {
        int current = getDepth();
        if (depth < 0 || depth > current) {
            throw new ItemNotFoundException("ancestor at depth " + depth
                    + " does not exist");
        }
        Tree ancestor = getTree();
        while (depth < current) {
            ancestor = ancestor.getParent();
            current -= 1;
        }
        return new NodeDelegate(sessionContext, ancestor);
    }

    Iterator<NodeDelegate> getChildren() throws RepositoryException {
        return nodeDelegateIterator(getTree().getChildren().iterator());
    }

    long getChildrenCount() throws RepositoryException {
        return getTree().getChildrenCount();
    }

    int getDepth() throws RepositoryException {
        return PathUtils.getDepth(getPath());
    }

    @Override
    String getName() {
        return getTree().getName();
    }

    Status getNodeStatus() {
        return getTree().getParent().getChildStatus(getName());
    }

    NodeDelegate getNodeOrNull(String relOakPath) {
        Tree tree = getTree(relOakPath);
        return tree == null ? null : new NodeDelegate(sessionContext, tree);
    }

    NodeDelegate getParent() throws RepositoryException {
        if (getTree().getParent() == null) {
            throw new ItemNotFoundException("Root has no parent");
        }

        return new NodeDelegate(sessionContext, getTree().getParent());
    }

    @Override
    String getPath() {
        return '/' + getTree().getPath();
    }

    Iterator<PropertyDelegate> getProperties() throws RepositoryException {
        return propertyDelegateIterator(getTree().getProperties().iterator());
    }

    long getPropertyCount() throws RepositoryException {
        return getTree().getPropertyCount();
    }

    PropertyDelegate getPropertyOrNull(String relOakPath)
            throws RepositoryException {

        Tree parent = getTree(PathUtils.getParentPath(relOakPath));
        if (parent == null) {
            return null;
        }

        String name = PathUtils.getName(relOakPath);
        PropertyState propertyState = parent.getProperty(name);
        return propertyState == null ? null : new PropertyDelegate(
                sessionContext, parent, propertyState);
    }

    SessionContext getSessionContext() {
        return sessionContext;
    }

    void remove() throws RepositoryException {
        getTree().getParent().removeChild(getName());
    }

    PropertyDelegate setProperty(String oakName, CoreValue value)
            throws RepositoryException {

        getTree().setProperty(oakName, value);
        return getPropertyOrNull(oakName);
    }

    PropertyDelegate setProperty(String oakName, List<CoreValue> value)
            throws RepositoryException {

        getTree().setProperty(oakName, value);
        return getPropertyOrNull(oakName);
    }

    // -----------------------------------------------------------< private >---

    private Tree getTree(String relPath) {
        Tree tree = getTree();
        for (String name : PathUtils.elements(relPath)) {
            if (tree == null) {
                return null;
            }
            tree = tree.getChild(name);
        }
        return tree;
    }

    private synchronized Tree getTree() {
        return tree = sessionContext.getTree(tree.getPath());
    }

    private Iterator<NodeDelegate> nodeDelegateIterator(
            Iterator<Tree> childNodeStates) {
        return Iterators.map(childNodeStates,
                new Function1<Tree, NodeDelegate>() {
                    @Override
                    public NodeDelegate apply(Tree state) {
                        return new NodeDelegate(sessionContext, state);
                    }
                });
    }

    private Iterator<PropertyDelegate> propertyDelegateIterator(
            Iterator<? extends PropertyState> properties) {
        return Iterators.map(properties,
                new Function1<PropertyState, PropertyDelegate>() {
                    @Override
                    public PropertyDelegate apply(PropertyState propertyState) {
                        return new PropertyDelegate(sessionContext, tree,
                                propertyState);
                    }
                });
    }
}
