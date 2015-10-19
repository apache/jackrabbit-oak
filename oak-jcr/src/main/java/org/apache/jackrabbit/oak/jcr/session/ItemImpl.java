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
package org.apache.jackrabbit.oak.jcr.session;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.PATH;
import static org.apache.jackrabbit.oak.api.Type.PATHS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.UNDEFINED;
import static org.apache.jackrabbit.oak.api.Type.UNDEFINEDS;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.ItemDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.ItemOperation;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
abstract class ItemImpl<T extends ItemDelegate> implements Item {
    private static final Logger log = LoggerFactory.getLogger(ItemImpl.class);

    public static final String ITEM_SAVE_DOES_SESSION_SAVE = "item-save-does-session-save";
    public static final int MV_PROPERTY_WARN_THRESHOLD = 1000;

    /**
     * The value of this flag determines the behaviour of {@link #save()}. If {@code false},
     * save will throw a {@link javax.jcr.UnsupportedRepositoryOperationException} if the
     * sub tree rooted at this item does not contain <em>all</em> transient changes. If
     * {@code true}, save will delegate to {@link Session#save()}.
     */
    public static final boolean SAVE_SESSION;
    static {
        String property = System.getProperty(ITEM_SAVE_DOES_SESSION_SAVE);
        SAVE_SESSION = property == null || Boolean.parseBoolean(property);
    }

    protected final SessionContext sessionContext;
    protected final T dlg;
    protected final SessionDelegate sessionDelegate;

    protected ItemImpl(T itemDelegate, SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.dlg = itemDelegate;
        this.sessionDelegate = sessionContext.getSessionDelegate();
    }

    protected abstract class ItemWriteOperation<U> extends SessionOperation<U> {
        protected ItemWriteOperation(String name) {
            super(name, true);
        }
        @Override
        public void checkPreconditions() throws RepositoryException {
            dlg.checkAlive();
            if (dlg.isProtected()) {
                throw new ConstraintViolationException("Item is protected.");
            }
        }
    }

    /**
     * Perform the passed {@link SessionOperation}.
     * @param op  operation to perform
     * @param <U>  return type of the operation
     * @return  the result of {@code op.perform()}
     * @throws RepositoryException as thrown by {@code op.perform()}.
     */
    @Nonnull
    protected final <U> U perform(@Nonnull SessionOperation<U> op) throws RepositoryException {
        return sessionDelegate.perform(op);
    }

    //---------------------------------------------------------------< Item >---

    /**
     * @see javax.jcr.Item#getName()
     */
    @Override
    @Nonnull
    public String getName() throws RepositoryException {
        String oakName = perform(new ItemOperation<String>(dlg, "getName") {
            @Nonnull
            @Override
            public String perform() {
                return item.getName();
            }
        });
        // special case name of root node
        return oakName.isEmpty() ? "" : toJcrPath(dlg.getName());
    }

    /**
     * @see javax.jcr.Property#getPath()
     */
    @Override
    @Nonnull
    public String getPath() throws RepositoryException {
        return toJcrPath(perform(new ItemOperation<String>(dlg, "getPath") {
            @Nonnull
            @Override
            public String perform() {
                return item.getPath();
            }
        }));
    }

    @Override @Nonnull
    public Session getSession() {
        return sessionContext.getSession();
    }

    @Override
    public Item getAncestor(final int depth) throws RepositoryException {
        if (depth < 0) {
            throw new ItemNotFoundException(
                    getPath() + "Invalid ancestor depth " + depth);
        } else if (depth == 0) {
            return sessionContext.getSession().getRootNode();
        }

        ItemDelegate ancestor = perform(new ItemOperation<ItemDelegate>(dlg, "getAncestor") {
            @Nonnull
            @Override
            public ItemDelegate perform() throws RepositoryException {
                String path = item.getPath();

                int slash = 0;
                for (int i = 0; i < depth - 1; i++) {
                    slash = PathUtils.getNextSlash(path, slash + 1);
                    if (slash == -1) {
                        throw new ItemNotFoundException(
                                path + ": Invalid ancestor depth " + depth);
                    }
                }
                slash = PathUtils.getNextSlash(path, slash + 1);
                if (slash == -1) {
                    return item;
                }

                NodeDelegate ndlg = sessionDelegate.getNode(path.substring(0, slash));
                if (ndlg == null) {
                    throw new ItemNotFoundException(getPath() + "Invalid ancestor depth " + depth);
                } else {
                    return ndlg;
                }
            }
        });

        if (ancestor == dlg) {
            return this;
        } else if (ancestor instanceof NodeDelegate) {
            return NodeImpl.createNode((NodeDelegate) ancestor, sessionContext);
        } else {
            throw new AccessDeniedException(
                    getPath() + ": Access denied to ancestor at depth " + depth);
        }
    }

    @Override
    public int getDepth() throws RepositoryException {
        return PathUtils.getDepth(getPath());
    }

    /**
     * @see Item#isSame(javax.jcr.Item)
     */
    @Override
    public boolean isSame(Item otherItem) throws RepositoryException {
        if (this == otherItem) {
            return true;
        }

        // The objects are either both Node objects or both Property objects.
        if (isNode() != otherItem.isNode()) {
            return false;
        }

        // Test if both items belong to the same repository
        // created by the same Repository object
        if (!getSession().getRepository().equals(otherItem.getSession().getRepository())) {
            return false;
        }

        // Both objects were acquired through Session objects bound to the same
        // repository workspace.
        if (!getSession().getWorkspace().getName().equals(otherItem.getSession().getWorkspace().getName())) {
            return false;
        }

        if (isNode()) {
            return ((Node) this).getIdentifier().equals(((Node) otherItem).getIdentifier());
        } else {
            return getName().equals(otherItem.getName()) && getParent().isSame(otherItem.getParent());
        }
    }

    /**
     * This implementation delegates to {@link Session#save()} if {@link #SAVE_SESSION} is
     * {@code true}. Otherwise it only performs the save if the subtree rooted at this item contains
     * all transient changes. That is, if calling {@link Session#save()} would have the same effect
     * as calling this method. In all other cases this method will throw an
     * {@link javax.jcr.UnsupportedRepositoryOperationException}
     *
     * @see javax.jcr.Item#save()
     *
     *
     */
    @Override
    public void save() throws RepositoryException {
        try {
            sessionDelegate.performVoid(new ItemWriteOperation<Void>("save") {
                @Override
                public void performVoid() throws RepositoryException {
                    dlg.save();
                }

                @Override
                public boolean isSave() {
                    return true;
                }
            });
        } catch (UnsupportedRepositoryOperationException e) {
            if (SAVE_SESSION) {
                if (isNew()) {
                    throw new RepositoryException("Item.save() not allowed on new item");
                }

                log.warn("Item#save is only supported when the subtree rooted at that item " +
                        "contains all transient changes. Falling back to Session#save since " +
                        "system property " + ITEM_SAVE_DOES_SESSION_SAVE + " is true.");
                getSession().save();
            } else {
                throw e;
            }
        }
    }

    /**
     * @see Item#refresh(boolean)
     */
    @Override
    public void refresh(final boolean keepChanges) throws RepositoryException {
        if (!keepChanges) {
            log.warn("Item#refresh invokes Session#refresh!");
        }
        sessionDelegate.performVoid(new SessionOperation<Void>("refresh") {
            @Override
            public void performVoid() throws InvalidItemStateException {
                sessionDelegate.refresh(keepChanges);
                if (!dlg.exists()) {
                    throw new InvalidItemStateException(
                            "This item no longer exists");
                }
            }

            @Override
            public boolean isUpdate() {
                return true;
            }

            @Override
            public boolean isRefresh() {
                return true;
            }
        });
    }

    @Override
    public String toString() {
        return (isNode() ? "Node[" : "Property[") + dlg + ']';
    }

    //-----------------------------------------------------------< internal >---
    @Nonnull
    String getOakName(String name) throws RepositoryException {
        return sessionContext.getOakName(name);
    }

    @Nonnull
    String getOakPathOrThrow(String jcrPath) throws RepositoryException {
        return sessionContext.getOakPathOrThrow(jcrPath);
    }

    @Nonnull
    String getOakPathOrThrowNotFound(String relPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(relPath);
    }

    @Nonnull
    String toJcrPath(String oakPath) {
        return sessionContext.getJcrPath(oakPath);
    }

    /**
     * Returns the value factory associated with the editing session.
     *
     * @return the value factory
     */
    @Nonnull
    ValueFactory getValueFactory() {
        return sessionContext.getValueFactory();
    }

    @Nonnull
    ReadWriteNodeTypeManager getNodeTypeManager() {
        return sessionContext.getWorkspace().getNodeTypeManager();
    }

    @Nonnull
    VersionManager getVersionManager() throws RepositoryException {
        return sessionContext.getWorkspace().getVersionManager();
    }

    @Nonnull
    protected PropertyState createSingleState(
            String oakName, Value value, Type<?> type)
            throws RepositoryException {
        if (type == UNDEFINED) {
            type = Type.fromTag(value.getType(), false);
        }
        if (type == NAME || type == PATH) {
            return createProperty(oakName, getOakValue(value, type), type);
        } else {
            return createProperty(oakName, value);
        }
    }

    @Nonnull
    protected PropertyState createMultiState(
            String oakName, List<Value> values, Type<?> type)
            throws RepositoryException {
        if (values.isEmpty()) {
            Type<?> base = type.getBaseType();
            if (base == UNDEFINED) {
                base = STRING;
            }
            return PropertyBuilder.array(base)
                    .setName(oakName).getPropertyState();
        }
        if (type == UNDEFINEDS) {
            type = Type.fromTag(values.get(0).getType(), true);
        }
        if (type == NAMES || type == PATHS) {
            Type<?> base = type.getBaseType();
            List<String> strings = newArrayListWithCapacity(values.size());
            for (Value value : values) {
                strings.add(getOakValue(value, base));
            }
            return createProperty(oakName, strings, type);
        } else {
            return createProperty(oakName, values, type.tag());
        }
    }

    private String getOakValue(Value value, Type<?> type)
            throws RepositoryException {
        if (type == NAME) {
            return getOakName(value.getString());
        } else if (type == PATH) {
            String path = value.getString();
            if (!path.startsWith("[")) { // leave identifiers unmodified
                path = getOakPathOrThrow(path);
            }
            return path;
        } else {
            throw new IllegalArgumentException();
        }
    }

}
