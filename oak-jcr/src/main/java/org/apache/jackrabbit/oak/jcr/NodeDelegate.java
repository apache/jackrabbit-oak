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

    /**
     * The underlying {@link Tree} instance. In order to ensure the instance
     * is up to date, this field <em>should not be accessed directly</em> but
     * rather the {@link #getTree()} method should be used.
     */
    private Tree tree;

    NodeDelegate(SessionDelegate sessionDelegate, Tree tree) {
        super(sessionDelegate);
        this.tree = tree;
    }

    @Override
    public String getName() {
        return getTree().getName();
    }

    @Override
    public String getPath() {
        return '/' + getTree().getPath();
    }

    @Override
    public NodeDelegate getParent() {
        Tree parent = getParentTree();
        return parent == null ? null : new NodeDelegate(sessionDelegate, parent);
    }

    @Override
    public boolean isStale() {
        return getTree() == null;
    }

    @Override
    public Status getStatus() {
        Tree parent = getParentTree();
        if (parent == null) {
            return Status.EXISTING;  // FIXME: return correct status for root
        }
        else {
            return parent.getChildStatus(getName());
        }
    }

    @Override
    public SessionDelegate getSessionDelegate() {
        return sessionDelegate;
    }

    @Override
    public String toString() {
        // don't disturb the state: avoid calling getTree()
        return "NodeDelegate[/" + tree.getPath() + ']';
    }

    public String getIdentifier() {
        // TODO for now the identifier is the OAK path
        return getPath();
    }

    /**
     * Determine whether this is the root node
     * @return  {@code true} iff this is the root node
     */
    public boolean isRoot() {
        return getParentTree() == null;
    }

    /**
     * Get the number of properties of the node
     * @return  number of properties of the node
     */
    public long getPropertyCount() {
        return getTree().getPropertyCount();
    }

    /**
     * Get a property
     * @param relPath  oak path
     * @return  property at the path given by {@code relPath} or {@code null} if
     * no such property exists
     */
    public PropertyDelegate getProperty(String relPath) {
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
     * Get the properties of the node
     * @return  properties of the node
     */
    public Iterator<PropertyDelegate> getProperties() {
        return propertyDelegateIterator(getTree().getProperties().iterator());
    }

    /**
     * Get the number of child nodes
     * @return  number of child nodes of the node
     */
    public long getChildCount() {
        return getTree().getChildrenCount();
    }

    /**
     * Get child node
     * @param relPath  oak path
     * @return  node at the path given by {@code relPath} or {@code null} if
     * no such node exists
     */
    public NodeDelegate getChild(String relPath) {
        Tree tree = getTree(relPath);
        return tree == null ? null : new NodeDelegate(sessionDelegate, tree);
    }

    /**
     * Get child nodes
     * @return  child nodes of the node
     */
    public Iterator<NodeDelegate> getChildren() {
        return nodeDelegateIterator(getTree().getChildren().iterator());
    }

    /**
     * Set a property
     * @param name  oak name
     * @param value
     * @return  the set property
     */
    public PropertyDelegate setProperty(String name, CoreValue value) {
        PropertyState propertyState = getTree().setProperty(name, value);
        return new PropertyDelegate(sessionDelegate, getTree(), propertyState);
    }

    /**
     * Set a multi valued property
     * @param name  oak name
     * @param value
     * @return  the set property
     */
    public PropertyDelegate setProperty(String name, List<CoreValue> value) {
        PropertyState propertyState = getTree().setProperty(name, value);
        return new PropertyDelegate(sessionDelegate, getTree(), propertyState);
    }

    /**
     * Add a child node
     * @param name  oak name
     * @return  the added node or {@code null} if such a node already exists
     */
    public NodeDelegate addChild(String name) {
        Tree tree = getTree();
        return tree.hasChild(name)
            ? null
            : new NodeDelegate(sessionDelegate, tree.addChild(name));
    }

    /**
     * Remove the node
     */
    public void remove() {
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
