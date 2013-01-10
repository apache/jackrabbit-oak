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

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Item;
import javax.jcr.ItemExistsException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.ItemVisitor;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.lock.Lock;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.ItemNameMatcher;
import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.PropertyIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeType;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.util.TODO;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.jcr.Property.JCR_LOCK_IS_DEEP;
import static javax.jcr.Property.JCR_LOCK_OWNER;

/**
 * TODO document
 * 
 * @param <T> the delegate type
 */
public class NodeImpl<T extends NodeDelegate> extends ItemImpl<T> implements Node {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NodeImpl.class);

    public NodeImpl(T dlg) {
        super(dlg.getSessionDelegate(), dlg);
    }

    //---------------------------------------------------------------< Item >---

    /**
     * @see javax.jcr.Item#isNode()
     */
    @Override
    public boolean isNode() {
        return true;
    }

    /**
     * @see javax.jcr.Item#getParent()
     */
    @Override
    @Nonnull
    public Node getParent() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeImpl<NodeDelegate>>() {
            @Override
            public NodeImpl<NodeDelegate> perform() throws RepositoryException {
                if (dlg.isRoot()) {
                    throw new ItemNotFoundException("Root has no parent");
                } else {
                    NodeDelegate parent = dlg.getParent();
                    if (parent == null) {
                        throw new AccessDeniedException();
                    }
                    return new NodeImpl<NodeDelegate>(parent);
                }
            }
        });
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        try {
            return sessionDelegate.perform(new SessionOperation<Boolean>() {
                @Override
                public Boolean perform() throws InvalidItemStateException {
                    return !dlg.isStale() && dlg.getStatus() == Status.NEW;
                }
            });
        }
        catch (RepositoryException e) {
            return false;
        }
    }

    /**
     * @see javax.jcr.Item#isModified()
     */
    @Override
    public boolean isModified() {
        try {
            return sessionDelegate.perform(new SessionOperation<Boolean>() {
                @Override
                public Boolean perform() throws InvalidItemStateException {
                    return !dlg.isStale() && dlg.getStatus() == Status.MODIFIED;
                }
            });
        }
        catch (RepositoryException e) {
            return false;
        }
    }

    /**
     * @see javax.jcr.Item#remove()
     */
    @Override
    public void remove() throws RepositoryException {
        checkStatus();
        checkProtected();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                if (dlg.isRoot()) {
                    throw new RepositoryException("Cannot remove the root node");
                }

                dlg.remove();
                return null;
            }
        });
    }

    /**
     * @see Item#accept(javax.jcr.ItemVisitor)
     */
    @Override
    public void accept(ItemVisitor visitor) throws RepositoryException {
        checkStatus();
        visitor.visit(this);
    }

    //---------------------------------------------------------------< Node >---
    /**
     * @see Node#addNode(String)
     */
    @Override
    @Nonnull
    public Node addNode(String relPath) throws RepositoryException {
        checkStatus();
        return addNode(relPath, null);
    }

    @Override
    @Nonnull
    public Node addNode(final String relPath, final String primaryNodeTypeName) throws RepositoryException {
        checkStatus();
        checkProtected();

        return sessionDelegate.perform(new SessionOperation<Node>() {
            @Override
            public Node perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathKeepIndexOrThrowNotFound(relPath);
                String oakName = PathUtils.getName(oakPath);
                String parentPath = sessionDelegate.getOakPathOrThrow(PathUtils.getParentPath(oakPath));

                // handle index
                if (oakName.contains("[")) {
                    throw new RepositoryException("Cannot create a new node using a name including an index");
                }

                NodeDelegate parent = dlg.getChild(parentPath);
                if (parent == null) {
                    // is it a property?
                    String grandParentPath = PathUtils.getParentPath(parentPath);
                    NodeDelegate grandParent = dlg.getChild(grandParentPath);
                    if (grandParent != null) {
                        String propName = PathUtils.getName(parentPath);
                        if (grandParent.getProperty(propName) != null) {
                            throw new ConstraintViolationException("Can't add new node to property.");
                        }
                    }

                    throw new PathNotFoundException(relPath);
                }

                if (parent.getChild(oakName) != null) {
                    throw new ItemExistsException(relPath);
                }

                String ntName = primaryNodeTypeName;
                if (ntName == null) {
                    DefinitionProvider dp = sessionDelegate.getDefinitionProvider();
                    try {
                        String childName = sessionDelegate.getOakNameOrThrow(PathUtils.getName(relPath));
                        NodeDefinition def = dp.getDefinition(new NodeImpl<NodeDelegate>(parent), childName);
                        ntName = def.getDefaultPrimaryTypeName();
                    } catch (RepositoryException e) {
                        throw new ConstraintViolationException(
                                "no matching child node definition found for " + relPath);
                    }
                }

                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                NodeType nt = ntm.getNodeType(ntName); // throws on not found
                if (nt.isAbstract() || nt.isMixin()) {
                    throw new ConstraintViolationException();
                }
                // TODO: END

                NodeDelegate added = parent.addChild(oakName);
                if (added == null) {
                    throw new ItemExistsException();
                }

                NodeImpl childNode = new NodeImpl<NodeDelegate>(added);
                childNode.internalSetPrimaryType(ntName);
                childNode.autoCreateItems();
                return childNode;
            }
        });
    }

    @Override
    public void orderBefore(final String srcChildRelPath, final String destChildRelPath) throws RepositoryException {
        checkStatus();
        checkProtected();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                String oakSrcChildRelPath =
                        sessionDelegate.getOakPathOrThrowNotFound(srcChildRelPath);
                String oakDestChildRelPath = null;
                if (destChildRelPath != null) {
                    oakDestChildRelPath =
                            sessionDelegate.getOakPathOrThrowNotFound(destChildRelPath);
                }
                dlg.orderBefore(oakSrcChildRelPath, oakDestChildRelPath);
                return null;
            }
        });
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, Value value) throws RepositoryException {
        int type = PropertyType.UNDEFINED;
        if (value != null) {
            type = value.getType();
        }
        return internalSetProperty(name, value, type, false);
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value, int)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, Value value, int type)
            throws RepositoryException {
        return internalSetProperty(name, value, type, type != PropertyType.UNDEFINED);
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value[])
     */
    @Override
    @Nonnull
    public Property setProperty(String name, Value[] values) throws RepositoryException {
        int type;
        if (values == null || values.length == 0 || values[0] == null) {
            type = PropertyType.UNDEFINED;
        } else {
            type = values[0].getType();
        }
        return internalSetProperty(name, values, type, false);
    }

    @Override
    @Nonnull
    public Property setProperty(String jcrName, Value[] values, int type) throws RepositoryException {
        return internalSetProperty(jcrName, values, type, true);
    }

    /**
     * @see Node#setProperty(String, String[])
     */
    @Override
    @Nonnull
    public Property setProperty(String name, String[] values) throws RepositoryException {
        return setProperty(name, values, PropertyType.UNDEFINED);
    }

    /**
     * @see Node#setProperty(String, String[], int)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, String[] values, int type) throws RepositoryException {
        Value[] vs;
        if (type == PropertyType.UNDEFINED) {
            vs = ValueHelper.convert(values, PropertyType.STRING, getValueFactory());
        } else {
            vs = ValueHelper.convert(values, type, getValueFactory());
        }
        return internalSetProperty(name, vs, type, (type != PropertyType.UNDEFINED));
    }

    /**
     * @see Node#setProperty(String, String)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, String value) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value, PropertyType.STRING);
        return internalSetProperty(name, v, PropertyType.UNDEFINED, false);
    }

    /**
     * @see Node#setProperty(String, String, int)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, String value, int type) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value, type);
        return internalSetProperty(name, v, type, true);
    }

    /**
     * @see Node#setProperty(String, InputStream)
     */
    @SuppressWarnings("deprecation")
    @Override
    @CheckForNull
    public Property setProperty(String name, InputStream value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.BINARY);
    }

    /**
     * @see Node#setProperty(String, Binary)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, Binary value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.BINARY);
    }

    /**
     * @see Node#setProperty(String, boolean)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, boolean value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.BOOLEAN);
    }

    /**
     * @see Node#setProperty(String, double)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, double value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.DOUBLE);
    }

    /**
     * @see Node#setProperty(String, BigDecimal)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, BigDecimal value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.DECIMAL);
    }

    /**
     * @see Node#setProperty(String, long)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, long value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.LONG);
    }

    /**
     * @see Node#setProperty(String, Calendar)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, Calendar value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.DATE);
    }

    /**
     * @see Node#setProperty(String, Node)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, Node value) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value);
        return setProperty(name, v, PropertyType.REFERENCE);
    }

    @Override
    @Nonnull
    public Node getNode(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeImpl>() {
            @Override
            public NodeImpl perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrowNotFound(relPath);

                NodeDelegate nd = dlg.getChild(oakPath);
                if (nd == null) {
                    throw new PathNotFoundException(relPath);
                } else {
                    return new NodeImpl<NodeDelegate>(nd);
                }
            }
        });
    }

    @Override
    @Nonnull
    public NodeIterator getNodes() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeIterator>() {
            @Override
            public NodeIterator perform() throws RepositoryException {
                Iterator<NodeDelegate> children = dlg.getChildren();
                long size = dlg.getChildCount();
                return new NodeIteratorAdapter(nodeIterator(children), size);
            }
        });
    }

    @Override
    @Nonnull
    public NodeIterator getNodes(final String namePattern)
            throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeIterator>() {
            @Override
            public NodeIterator perform() throws RepositoryException {
                Iterator<NodeDelegate> children = Iterators.filter(dlg.getChildren(),
                        new Predicate<NodeDelegate>() {
                            @Override
                            public boolean apply(NodeDelegate state) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(state.getName()), namePattern);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new NodeIteratorAdapter(nodeIterator(children));
            }
        });
    }

    @Override
    @Nonnull
    public NodeIterator getNodes(final String[] nameGlobs) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeIterator>() {
            @Override
            public NodeIterator perform() throws RepositoryException {
                Iterator<NodeDelegate> children = Iterators.filter(dlg.getChildren(),
                        new Predicate<NodeDelegate>() {
                            @Override
                            public boolean apply(NodeDelegate state) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(state.getName()), nameGlobs);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new NodeIteratorAdapter(nodeIterator(children));
            }
        });
    }

    @Override
    @Nonnull
    public Property getProperty(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyImpl>() {
            @Override
            public PropertyImpl perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrowNotFound(relPath);
                PropertyDelegate pd = dlg.getProperty(oakPath);
                if (pd == null) {
                    throw new PathNotFoundException(relPath + " not found on " + getPath());
                } else {
                    return new PropertyImpl(pd);
                }
            }
        });
    }

    @Override
    @Nonnull
    public PropertyIterator getProperties() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws RepositoryException {
                Iterator<PropertyDelegate> properties = dlg.getProperties();
                long size = dlg.getPropertyCount();
                return new PropertyIteratorAdapter(propertyIterator(properties), size);
            }
        });
    }

    @Override
    @Nonnull
    public PropertyIterator getProperties(final String namePattern) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws RepositoryException {
                Iterator<PropertyDelegate> properties = Iterators.filter(dlg.getProperties(),
                        new Predicate<PropertyDelegate>() {
                            @Override
                            public boolean apply(PropertyDelegate entry) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(entry.getName()), namePattern);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new PropertyIteratorAdapter(propertyIterator(properties));
            }
        });
    }

    @Override
    @Nonnull
    public PropertyIterator getProperties(final String[] nameGlobs) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws RepositoryException {
                Iterator<PropertyDelegate> propertyNames = Iterators.filter(dlg.getProperties(),
                        new Predicate<PropertyDelegate>() {
                            @Override
                            public boolean apply(PropertyDelegate entry) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(entry.getName()), nameGlobs);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new PropertyIteratorAdapter(propertyIterator(propertyNames));
            }
        });
    }

    /**
     * @see javax.jcr.Node#getPrimaryItem()
     */
    @Override
    @Nonnull
    public Item getPrimaryItem() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Item>() {
            @Override
            public Item perform() throws RepositoryException {
                String name = getPrimaryNodeType().getPrimaryItemName();
                if (name == null) {
                    throw new ItemNotFoundException("No primary item present on node " + this);
                }
                if (hasProperty(name)) {
                    return getProperty(name);
                } else if (hasNode(name)) {
                    return getNode(name);
                } else {
                    throw new ItemNotFoundException("Primary item " + name + " does not exist on node " + this);
                }
            }
        });
    }

    /**
     * @see javax.jcr.Node#getUUID()
     */
    @Override
    @Nonnull
    public String getUUID() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                if (isNodeType(NodeType.MIX_REFERENCEABLE)) {
                    return getIdentifier();
                }

                throw new UnsupportedRepositoryOperationException("Node is not referenceable.");
            }
        });
    }

    @Override
    @Nonnull
    public String getIdentifier() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return dlg.getIdentifier();
            }
        });
    }

    @Override
    public int getIndex() throws RepositoryException {
        // as long as we do not support same name siblings, index always is 1
        return 1;
    }

    /**
     * @see javax.jcr.Node#getReferences()
     */
    @Override
    @Nonnull
    public PropertyIterator getReferences() throws RepositoryException {
        return getReferences(null);
    }

    @Override
    @Nonnull
    public PropertyIterator getReferences(final String name) throws RepositoryException {
        checkStatus();
        return internalGetReferences(name, false);
    }

    /**
     * @see javax.jcr.Node#getWeakReferences()
     */
    @Override
    @Nonnull
    public PropertyIterator getWeakReferences() throws RepositoryException {
        return getWeakReferences(null);
    }

    @Override
    @Nonnull
    public PropertyIterator getWeakReferences(String name) throws RepositoryException {
        checkStatus();
        return internalGetReferences(name, true);
    }

    private PropertyIterator internalGetReferences(final String name, final boolean weak) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws InvalidItemStateException {
                IdentifierManager idManager = sessionDelegate.getIdManager();

                Set<String> propertyOakPaths = idManager.getReferences(weak, dlg.getTree(), name);
                Iterable<Property> properties = Iterables.transform(
                    propertyOakPaths,
                    new Function<String, Property>() {
                        @Override
                        public Property apply(String oakPath) {
                            PropertyDelegate pd = sessionDelegate.getProperty(oakPath);
                            return pd == null ? null : new PropertyImpl(pd);
                        }
                    }
                );

                return new PropertyIteratorAdapter(properties.iterator(), propertyOakPaths.size());
            }
        });
    }

    @Override
    public boolean hasNode(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrow(relPath);
                return dlg.getChild(oakPath) != null;
            }
        });
    }

    @Override
    public boolean hasProperty(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrow(relPath);
                return dlg.getProperty(oakPath) != null;
            }
        });
    }

    @Override
    public boolean hasNodes() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return dlg.getChildCount() != 0;
            }
        });
    }

    @Override
    public boolean hasProperties() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return dlg.getPropertyCount() != 0;
            }
        });
    }

    /**
     * @see javax.jcr.Node#getPrimaryNodeType()
     */
    @Override
    @Nonnull
    public NodeType getPrimaryNodeType() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeType>() {
            @Override
            public NodeType perform() throws RepositoryException {
                NodeTypeManager ntMgr = sessionDelegate.getNodeTypeManager();
                String primaryNtName;
                if (hasProperty(Property.JCR_PRIMARY_TYPE)) {
                    primaryNtName = getProperty(Property.JCR_PRIMARY_TYPE).getString();
                } else {
                    throw new RepositoryException("Node " + getPath() + " doesn't have primary type set.");
                }
                return ntMgr.getNodeType(primaryNtName);
            }
        });
    }

    /**
     * @see javax.jcr.Node#getMixinNodeTypes()
     */
    @Override
    @Nonnull
    public NodeType[] getMixinNodeTypes() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeType[]>() {
            @Override
            public NodeType[] perform() throws RepositoryException {
                // TODO: check if transient changes to mixin-types are reflected here
                if (hasProperty(Property.JCR_MIXIN_TYPES)) {
                    NodeTypeManager ntMgr = sessionDelegate.getNodeTypeManager();
                    Value[] mixinNames = getProperty(Property.JCR_MIXIN_TYPES).getValues();
                    NodeType[] mixinTypes = new NodeType[mixinNames.length];
                    for (int i = 0; i < mixinNames.length; i++) {
                        mixinTypes[i] = ntMgr.getNodeType(mixinNames[i].getString());
                    }
                    return mixinTypes;
                } else {
                    return new NodeType[0];
                }
            }
        });
    }

    @Override
    public boolean isNodeType(final String nodeTypeName) throws RepositoryException {
        checkStatus();

        String oakName = sessionDelegate.getOakNameOrThrow(nodeTypeName);
        return sessionDelegate.getEffectiveNodeTypeProvider().isNodeType(dlg.getTree(), oakName);
    }

    @Override
    public void setPrimaryType(final String nodeTypeName) throws RepositoryException {
        checkStatus();
        checkProtected();
        if (!isCheckedOut()) {
            throw new VersionException("Cannot set primary type. Node is " +
                    "checked in.");
        }

        internalSetPrimaryType(nodeTypeName);
    }

    @Override
    public void addMixin(final String mixinName) throws RepositoryException {
        checkStatus();
        checkProtected();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                ntm.getNodeType(mixinName); // throws on not found
                // TODO: END

                PropertyDelegate mixins = dlg.getProperty(JcrConstants.JCR_MIXINTYPES);
                Value value = sessionDelegate.getValueFactory().createValue(mixinName, PropertyType.NAME);

                boolean nodeModified = false;
                if (mixins == null) {
                    nodeModified = true;
                    dlg.setProperty(JcrConstants.JCR_MIXINTYPES, Collections.singletonList(value));
                } else {
                    List<Value> values = mixins.getValues();
                    if (!values.contains(value)) {
                        values.add(value);
                        nodeModified = true;
                        dlg.setProperty(JcrConstants.JCR_MIXINTYPES, values);
                    }
                }

                if (nodeModified) {
                    autoCreateItems();
                }
                return null;
            }
        });
    }

    @Override
    public void removeMixin(final String mixinName) throws RepositoryException {
        checkStatus();
        checkProtected();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                if (!isNodeType(mixinName)) {
                    throw new NoSuchNodeTypeException();
                }

                throw new ConstraintViolationException();
            }
        });
    }

    @Override
    public boolean canAddMixin(final String mixinName) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                ntm.getNodeType(mixinName); // throws on not found
                // TODO: END

                return isSupportedMixinName(mixinName) && isCheckedOut();
            }
        });
    }

    @Override
    @Nonnull
    public NodeDefinition getDefinition() throws RepositoryException {
        if (getDepth() == 0) {
            return dlg.sessionDelegate.getDefinitionProvider().getRootDefinition();
        } else {
            return dlg.sessionDelegate.getDefinitionProvider().getDefinition(getParent(), this);
        }
    }

    @Override
    @Nonnull
    public String getCorrespondingNodePath(String workspaceName) throws RepositoryException {
        checkStatus();
        checkValidWorkspace(workspaceName);
        throw new UnsupportedRepositoryOperationException("TODO: Node.getCorrespondingNodePath");
    }


    @Override
    public void update(String srcWorkspace) throws RepositoryException {
        checkStatus();
        checkValidWorkspace(srcWorkspace);
        ensureNoPendingSessionChanges();

        // TODO
    }

    /**
     * @see javax.jcr.Node#checkin()
     */
    @Override
    @Nonnull
    public Version checkin() throws RepositoryException {
        return sessionDelegate.getVersionManager().checkin(getPath());
    }

    /**
     * @see javax.jcr.Node#checkout()
     */
    @Override
    public void checkout() throws RepositoryException {
        sessionDelegate.getVersionManager().checkout(getPath());
    }

    /**
     * @see javax.jcr.Node#doneMerge(javax.jcr.version.Version)
     */
    @Override
    public void doneMerge(Version version) throws RepositoryException {
        sessionDelegate.getVersionManager().doneMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#cancelMerge(javax.jcr.version.Version)
     */
    @Override
    public void cancelMerge(Version version) throws RepositoryException {
        sessionDelegate.getVersionManager().cancelMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#merge(String, boolean)
     */
    @Override
    @Nonnull
    public NodeIterator merge(String srcWorkspace, boolean bestEffort) throws RepositoryException {
        return sessionDelegate.getVersionManager().merge(getPath(), srcWorkspace, bestEffort);
    }

    /**
     * @see javax.jcr.Node#isCheckedOut()
     */
    @Override
    public boolean isCheckedOut() throws RepositoryException {
        try {
            return sessionDelegate.getVersionManager().isCheckedOut(getPath());
        } catch (UnsupportedRepositoryOperationException ex) {
            // when versioning is not supported all nodes are considered to be
            // checked out
            return true;
        }
    }

    /**
     * @see javax.jcr.Node#restore(String, boolean)
     */
    @Override
    public void restore(String versionName, boolean removeExisting) throws RepositoryException {
        sessionDelegate.getVersionManager().restore(getPath(), versionName, removeExisting);
    }

    /**
     * @see javax.jcr.Node#restore(javax.jcr.version.Version, boolean)
     */
    @Override
    public void restore(Version version, boolean removeExisting) throws RepositoryException {
        sessionDelegate.getVersionManager().restore(version, removeExisting);
    }

    /**
     * @see javax.jcr.Node#restore(Version, String, boolean)
     */
    @Override
    public void restore(Version version, String relPath, boolean removeExisting) throws RepositoryException {
        // additional checks are performed with subsequent calls.
        if (hasNode(relPath)) {
            // node at 'relPath' exists -> call restore on the target Node
            getNode(relPath).restore(version, removeExisting);
        } else {
            // TODO
        }
    }

    /**
     * @see javax.jcr.Node#restoreByLabel(String, boolean)
     */
    @Override
    public void restoreByLabel(String versionLabel, boolean removeExisting) throws RepositoryException {
        sessionDelegate.getVersionManager().restoreByLabel(getPath(), versionLabel, removeExisting);
    }

    /**
     * @see javax.jcr.Node#getVersionHistory()
     */
    @Override
    @Nonnull
    public VersionHistory getVersionHistory() throws RepositoryException {
        return sessionDelegate.getVersionManager().getVersionHistory(getPath());
    }

    /**
     * @see javax.jcr.Node#getBaseVersion()
     */
    @Override
    @Nonnull
    public Version getBaseVersion() throws RepositoryException {
        return sessionDelegate.getVersionManager().getBaseVersion(getPath());
    }

    /**
     * Checks whether this node is locked by looking for the
     * {@code jcr:lockOwner} property either on this node or
     * on any ancestor that also has the {@code jcr:lockIsDeep}
     * property set to {@code true}.
     */
    @Override
    public boolean isLocked() throws RepositoryException {
        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = sessionDelegate.getOakPathOrThrow(JCR_LOCK_IS_DEEP);

        if (dlg.getProperty(lockOwner) != null) {
            return true;
        }

        NodeDelegate parent = dlg.getParent();
        while (parent != null) {
            if (parent.getProperty(lockOwner) != null) {
                PropertyDelegate isDeep = parent.getProperty(lockIsDeep);
                if (isDeep != null && !isDeep.isMultivalue()
                        && isDeep.getValue().getBoolean()) {
                    return true;
                }
            }
            parent = parent.getParent();
        }

        return false;
    }

    /**
     * Checks whether this node holds a lock by looking for the
     * {@code jcr:lockOwner} property.
     */
    @Override
    public boolean holdsLock() throws RepositoryException {
        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        return dlg.getProperty(lockOwner) != null;
    }

    /**
     * @see javax.jcr.Node#getLock()
     */
    @Override
    @Nonnull
    public Lock getLock() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * @see javax.jcr.Node#lock(boolean, boolean)
     */
    @Override
    @Nonnull
    public Lock lock(final boolean isDeep, boolean isSessionScoped)
            throws RepositoryException {
        final String userID = getSession().getUserID();

        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = sessionDelegate.getOakPathOrThrow(JCR_LOCK_IS_DEEP);
        try {
            ContentSession session = sessionDelegate.getContentSession();
            Root root = session.getLatestRoot();
            Tree tree = root.getTree(dlg.getPath());
            if (tree == null) {
                throw new ItemNotFoundException();
            }
            tree.setProperty(lockOwner, userID);
            tree.setProperty(lockIsDeep, isDeep);
            root.commit(); // TODO: fail instead?
        } catch (CommitFailedException e) {
            throw new RepositoryException("Unable to lock " + this, e);
        }

        getSession().refresh(true);

        if (isSessionScoped) {
            return TODO.dummyImplementation().returnValue(new Lock() {
                @Override
                public String getLockOwner() {
                    return userID;
                }

                @Override
                public boolean isDeep() {
                    return isDeep;
                }

                @Override
                public Node getNode() {
                    return NodeImpl.this;
                }

                @Override
                public String getLockToken() {
                    return null;
                }

                @Override
                public long getSecondsRemaining() {
                    return Long.MAX_VALUE;
                }

                @Override
                public boolean isLive() {
                    return true;
                }

                @Override
                public boolean isSessionScoped() {
                    return true;
                }

                @Override
                public boolean isLockOwningSession() {
                    return true;
                }

                @Override
                public void refresh() {
                }
            });
        }

        return getLock();
    }

    /**
     * @see javax.jcr.Node#unlock()
     */
    @Override
    public void unlock() throws RepositoryException {
        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = sessionDelegate.getOakPathOrThrow(JCR_LOCK_IS_DEEP);
        try {
            Root root = sessionDelegate.getContentSession().getLatestRoot();
            Tree tree = root.getTree(dlg.getPath());
            if (tree == null) {
                throw new ItemNotFoundException();
            }
            tree.removeProperty(lockOwner);
            tree.removeProperty(lockIsDeep);
            root.commit();
        } catch (CommitFailedException e) {
            throw new RepositoryException("Unable to unlock " + this, e);
        }

        getSession().refresh(true);
    }

    @Override
    @Nonnull
    public NodeIterator getSharedSet() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("TODO: Node.getSharedSet");
    }

    @Override
    public void removeSharedSet() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("TODO: Node.removeSharedSet");
    }

    @Override
    public void removeShare() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("TODO: Node.removeShare");
    }

    /**
     * @see javax.jcr.Node#followLifecycleTransition(String)
     */
    @Override
    public void followLifecycleTransition(String transition) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Lifecycle Management is not supported");
    }

    /**
     * @see javax.jcr.Node#getAllowedLifecycleTransistions()
     */
    @Override
    @Nonnull
    public String[] getAllowedLifecycleTransistions() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Lifecycle Management is not supported");

    }

    //------------------------------------------------------------< private >---

    private static Iterator<Node> nodeIterator(Iterator<NodeDelegate> childNodes) {
        return Iterators.transform(
                childNodes,
                new Function<NodeDelegate, Node>() {
                    @Override
                    public Node apply(NodeDelegate nodeDelegate) {
                        return new NodeImpl<NodeDelegate>(nodeDelegate);
                    }
                });
    }

    private static Iterator<Property> propertyIterator(Iterator<PropertyDelegate> properties) {
        return Iterators.transform(
                properties,
                new Function<PropertyDelegate, Property>() {
                    @Override
                    public Property apply(PropertyDelegate propertyDelegate) {
                        return new PropertyImpl(propertyDelegate);
                    }
                });
    }

    private static int getTargetType(Value value, PropertyDefinition definition) {
        if (definition.getRequiredType() == PropertyType.UNDEFINED) {
            return value.getType();
        } else {
            return definition.getRequiredType();
        }
    }

    private static int getTargetType(Value[] values, PropertyDefinition definition) {
        if (definition.getRequiredType() == PropertyType.UNDEFINED) {
            if (values.length != 0) {
                for (Value v : values) {
                    if (v != null) {
                        return v.getType();
                    }
                }
            }
            return PropertyType.STRING;
        } else {
            return definition.getRequiredType();
        }
    }

    private void autoCreateItems() throws RepositoryException {
        EffectiveNodeType effective = dlg.sessionDelegate.getEffectiveNodeTypeProvider().getEffectiveNodeType(this);
        for (PropertyDefinition pd : effective.getAutoCreatePropertyDefinitions()) {
            if (dlg.getProperty(pd.getName()) == null) {
                if (pd.isMultiple()) {
                    dlg.setProperty(pd.getName(), getAutoCreatedValues(pd));
                } else {
                    dlg.setProperty(pd.getName(), getAutoCreatedValue(pd));
                }
            }
        }
        for (NodeDefinition nd : effective.getAutoCreateNodeDefinitions()) {
            if (dlg.getChild(nd.getName()) == null) {
                autoCreateNode(nd);
            }
        }
    }

    private Value getAutoCreatedValue(PropertyDefinition definition)
            throws RepositoryException {
        String name = definition.getName();
        String declaringNT = definition.getDeclaringNodeType().getName();

        if (NodeTypeConstants.JCR_UUID.equals(name)) {
            // jcr:uuid property of the mix:referenceable node type
            if (NodeTypeConstants.MIX_REFERENCEABLE.equals(declaringNT)) {
                return getValueFactory().createValue(IdentifierManager.generateUUID());
            }
        } else if (NodeTypeConstants.JCR_CREATED.equals(name)) {
            // jcr:created property of a version or a mix:created
            if (NodeTypeConstants.MIX_CREATED.equals(declaringNT)
                    || NodeTypeConstants.NT_VERSION.equals(declaringNT)) {
                return getValueFactory().createValue(Calendar.getInstance());
            }
        } else if (NodeTypeConstants.JCR_CREATEDBY.equals(name)) {
            String userID = sessionDelegate.getAuthInfo().getUserID();
            // jcr:createdBy property of a mix:created
            if (userID != null
                    && NodeTypeConstants.MIX_CREATED.equals(declaringNT)) {
                return getValueFactory().createValue(userID);
            }
        } else if (NodeTypeConstants.JCR_LASTMODIFIED.equals(name)) {
            // jcr:lastModified property of a mix:lastModified
            if (NodeTypeConstants.MIX_LASTMODIFIED.equals(declaringNT)) {
                return getValueFactory().createValue(Calendar.getInstance());
            }
        } else if (NodeTypeConstants.JCR_LASTMODIFIEDBY.equals(name)) {
            String userID = sessionDelegate.getAuthInfo().getUserID();
            // jcr:lastModifiedBy property of a mix:lastModified
            if (userID != null
                    && NodeTypeConstants.MIX_LASTMODIFIED.equals(declaringNT)) {
                return getValueFactory().createValue(userID);
            }
        }
        // does the definition have a default value?
        if (definition.getDefaultValues() != null) {
            Value[] values = definition.getDefaultValues();
            if (values.length > 0) {
                return values[0];
            }
        }
        throw new RepositoryException("Unable to auto-create value for " +
                PathUtils.concat(getPath(), name));
    }

    private Iterable<Value> getAutoCreatedValues(PropertyDefinition definition)
            throws RepositoryException {
        String name = definition.getName();

        // default values?
        if (definition.getDefaultValues() != null) {
            return Lists.newArrayList(definition.getDefaultValues());
        }
        throw new RepositoryException("Unable to auto-create value for " +
                PathUtils.concat(getPath(), name));
    }

    private void autoCreateNode(NodeDefinition definition)
            throws RepositoryException {
        addNode(definition.getName(), definition.getDefaultPrimaryTypeName());
    }

    // FIXME OAK-505: hack to filter for a subset of supported mixins for now
    // this allows only harmless mixin types so that other code like addMixin gets test coverage
    private boolean isSupportedMixinName(String mixinName) throws RepositoryException {
        String oakName = sessionDelegate.getOakPathOrThrow(mixinName);
        return "mix:title".equals(oakName) ||
            NodeTypeConstants.MIX_REFERENCEABLE.equals(oakName) ||
            NodeTypeConstants.MIX_VERSIONABLE.equals(oakName) ||
            NodeTypeConstants.MIX_LOCKABLE.equals(oakName);
    }

    private void checkValidWorkspace(String workspaceName) throws RepositoryException {
        for (String wn : sessionDelegate.getWorkspace().getAccessibleWorkspaceNames()) {
            if (wn.equals(workspaceName)) {
                return;
            }
        }
        throw new NoSuchWorkspaceException(workspaceName + " does not exist.");
    }

    private void internalSetPrimaryType(final String nodeTypeName) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                NodeType nt = ntm.getNodeType(nodeTypeName); // throws on not found
                if (nt.isAbstract() || nt.isMixin()) {
                    throw new ConstraintViolationException();
                }
                // TODO: END

                String jcrPrimaryType = sessionDelegate.getOakPathOrThrow(Property.JCR_PRIMARY_TYPE);
                Value value = sessionDelegate.getValueFactory().createValue(nodeTypeName, PropertyType.NAME);
                dlg.setProperty(jcrPrimaryType, value);
                return null;
            }
        });
    }

    private Property internalSetProperty(final String jcrName, final Value value,
                                         final int type, final boolean exactTypeMatch) throws RepositoryException {
        checkStatus();
        checkProtected();

        return sessionDelegate.perform(new SessionOperation<Property>() {
            @Override
            public Property perform() throws RepositoryException {
                String oakName = sessionDelegate.getOakPathOrThrow(jcrName);
                if (value == null) {
                    if (hasProperty(jcrName)) {
                        Property property = getProperty(jcrName);
                        property.remove();
                        return property;
                    } else {
                        // Return a property instance which throws on access. See OAK-395
                        return new PropertyImpl(new PropertyDelegate(
                                sessionDelegate, dlg.getLocation().getChild(oakName)));
                    }
                } else {
                    PropertyDefinition definition;
                    if (hasProperty(jcrName)) {
                        definition = getProperty(jcrName).getDefinition();
                    } else {
                        definition = dlg.sessionDelegate.getDefinitionProvider().getDefinition(NodeImpl.this, oakName, false, type, exactTypeMatch);
                    }
                    checkProtected(definition);
                    if (definition.isMultiple()) {
                        throw new ValueFormatException("Cannot set single value to multivalued property");
                    }

                    int targetType = getTargetType(value, definition);
                    Value targetValue = ValueHelper.convert(value, targetType, getValueFactory());

                    return new PropertyImpl(dlg.setProperty(oakName, targetValue));
                }
            }
        });
    }

    private Property internalSetProperty(final String jcrName, final Value[] values,
                                         final int type, final boolean exactTypeMatch) throws RepositoryException {
        checkStatus();
        checkProtected();

        return sessionDelegate.perform(new SessionOperation<Property>() {
            @Override
            public Property perform() throws RepositoryException {
                String oakName = sessionDelegate.getOakPathOrThrow(jcrName);
                if (values == null) {
                    if (hasProperty(jcrName)) {
                        Property property = getProperty(jcrName);
                        property.remove();
                        return property;
                    } else {
                        return new PropertyImpl(new PropertyDelegate(
                                sessionDelegate, dlg.getLocation().getChild(oakName)));
                    }
                } else {
                    PropertyDefinition definition;
                    if (hasProperty(jcrName)) {
                        definition = getProperty(jcrName).getDefinition();
                    } else {
                        definition = dlg.sessionDelegate.getDefinitionProvider().getDefinition(NodeImpl.this, oakName, true, type, exactTypeMatch);
                    }
                    checkProtected(definition);
                    if (!definition.isMultiple()) {
                        throw new ValueFormatException("Cannot set value array to single value property");
                    }

                    int targetType = getTargetType(values, definition);
                    Value[] targetValues = ValueHelper.convert(values, targetType, getValueFactory());

                    Iterable<Value> nonNullValues = Iterables.filter(
                            Arrays.asList(targetValues),
                            Predicates.notNull());
                    return new PropertyImpl(dlg.setProperty(oakName, nonNullValues));
                }
            }
        });
    }
}
