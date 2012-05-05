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

import java.util.Iterator;
import java.util.List;

/**
 * {@code NodeDelegate} serve as internal representations of {@code Node}s.
 * The methods of this class do not throw checked exceptions. Instead clients
 * are expected to inspect the return value and ensure that all preconditions
 * hold before a method is invoked. Specifically the behaviour of all methods
 * of this class but {@link #isStale()} is undefined if the instance is stale.
 * An item is stale if the underlying items does not exist anymore.
 */
public class NodeDelegate extends ItemDelegate {
    private final SessionDelegate sessionDelegate;
    private Tree tree;

    NodeDelegate(SessionDelegate sessionDelegate, Tree tree) {
        this.sessionDelegate = sessionDelegate;
        this.tree = tree;
    }

    /**
     * Get the name of this node
     * @return oak name of the node
     */
    @Override
    String getName() {
        return getTree().getName();
    }

    /**
     * Get the path of this node
     * @return oak path of the node
     */
    @Override
    String getPath() {
        return '/' + getTree().getPath();
    }

    /**
     * Determine whether this node is stale
     * @return  {@code true} iff stale
     */
    @Override
    boolean isStale() {
        return getTree() == null;
    }

    /**
     * Determine whether this is the root node
     * @return  {@code true} iff this is the root node
     */
    boolean isRoot() {
        return getParentTree() == null;
    }

    /**
     * Get the status of this node
     * @return  {@link Status} of this node
     */
    Status getStatus() {
        Tree parent = getParentTree();
        if (parent == null) {
            return Status.EXISTING;  // FIXME: return correct status for root
        }
        else {
            return parent.getChildStatus(getName());
        }
    }

    /**
     * Get the session which with this node is associated
     * @return  {@link SessionDelegate} to which this node belongs
     */
    SessionDelegate getSessionDelegate() {
        return sessionDelegate;
    }

    /**
     * Get the parent of this node
     * @return  parent of this node or {@code null} it this is the root
     */
    NodeDelegate getParent() {
        Tree parent = getParentTree();
        return parent == null ? null : new NodeDelegate(sessionDelegate, parent);
    }

    /**
     * Get the number of properties of this node
     * @return  number of properties of this node
     */
    long getPropertyCount() {
        return getTree().getPropertyCount();
    }

    /**
     * Get a property
     * @param relPath  oak path
     * @return  property at the path given by {@code relPath} or {@code null} if
     * no such property exists
     */
    PropertyDelegate getProperty(String relPath) {
        Tree parent = getTree(PathUtils.getParentPath(relPath));
        if (parent == null) {
            return null;
        }

        String name = PathUtils.getName(relPath);
        PropertyState propertyState = parent.getProperty(name);
        return propertyState == null
            ? null
            : new PropertyDelegate(sessionDelegate, parent, propertyState);
    }

    /**
     * Get the properties of this node
     * @return  properties of this node
     */
    Iterator<PropertyDelegate> getProperties() {
        return propertyDelegateIterator(getTree().getProperties().iterator());
    }

    /**
     * Get the number of child nodes
     * @return  number of child nodes of this node
     */
    long getChildCount() {
        return getTree().getChildrenCount();
    }

    /**
     * Get child node
     * @param relPath  oak path
     * @return  node at the path given by {@code relPath} or {@code null} if
     * no such node exists
     */
    NodeDelegate getChild(String relPath) {
        Tree tree = getTree(relPath);
        return tree == null ? null : new NodeDelegate(sessionDelegate, tree);
    }

    /**
     * Get child nodes
     * @return  child nodes of this node
     */
    Iterator<NodeDelegate> getChildren() {
        return nodeDelegateIterator(getTree().getChildren().iterator());
    }

    /**
     * Set a property
     * @param name  oak name
     * @param value
     * @return  the set property
     */
    PropertyDelegate setProperty(String name, CoreValue value) {
        PropertyState propertyState = getTree().setProperty(name, value);
        return new PropertyDelegate(sessionDelegate, getTree(), propertyState);
    }

    /**
     * Set a multi valued property
     * @param name  oak name
     * @param value
     * @return  the set property
     */
    PropertyDelegate setProperty(String name, List<CoreValue> value) {
        PropertyState propertyState = getTree().setProperty(name, value);
        return new PropertyDelegate(sessionDelegate, getTree(), propertyState);
    }

    /**
     * Add a child node
     * @param name  oak name
     * @return  the added node or {@code null} if such a node already exists
     */
    NodeDelegate addChild(String name) {
        Tree tree = getTree();
        return tree.hasChild(name)
            ? null
            : new NodeDelegate(sessionDelegate, tree.addChild(name));
    }

    /**
     * Remove this node
     */
    void remove() {
        getParentTree().removeChild(getName());
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

    private Tree getParentTree() {
        return getTree().getParent();
    }

    private synchronized Tree getTree() {
        return tree = sessionDelegate.getTree(tree.getPath());
    }

    private Iterator<NodeDelegate> nodeDelegateIterator(
            Iterator<Tree> childNodeStates) {
        return Iterators.map(childNodeStates,
                new Function1<Tree, NodeDelegate>() {
                    @Override
                    public NodeDelegate apply(Tree state) {
                        return new NodeDelegate(sessionDelegate, state);
                    }
                });
    }

    private Iterator<PropertyDelegate> propertyDelegateIterator(
            Iterator<? extends PropertyState> properties) {
        return Iterators.map(properties,
                new Function1<PropertyState, PropertyDelegate>() {
                    @Override
                    public PropertyDelegate apply(PropertyState propertyState) {
                        return new PropertyDelegate(sessionDelegate, tree,
                                propertyState);
                    }
                });
    }
}
