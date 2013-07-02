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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.ItemDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionOperation;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
abstract class ItemImpl<T extends ItemDelegate> implements Item {
    private static final Logger log = LoggerFactory.getLogger(ItemImpl.class);

    protected final SessionContext sessionContext;
    protected final T dlg;
    protected final SessionDelegate sessionDelegate;

    private long updateCount;

    protected ItemImpl(T itemDelegate, SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.dlg = itemDelegate;
        this.sessionDelegate = sessionContext.getSessionDelegate();
        this.updateCount = sessionDelegate.getUpdateCount();
    }

    protected abstract class ItemReadOperation<U> extends SessionOperation<U> {
        @Override
        protected void checkPreconditions() throws RepositoryException {
            checkAlive();
        }
    }

    protected abstract class ItemWriteOperation<U> extends SessionOperation<U> {
        protected ItemWriteOperation() {
            super(true);
        }
        @Override
        protected void checkPreconditions() throws RepositoryException {
            checkAlive();
            checkProtected();
        }
    }

    /**
     * Perform the passed {@link org.apache.jackrabbit.oak.jcr.ItemImpl.ItemReadOperation}.
     * @param op  operation to perform
     * @param <U>  return type of the operation
     * @return  the result of {@code op.perform()}
     * @throws RepositoryException as thrown by {@code op.perform()}.
     */
    @CheckForNull
    protected final <U> U perform(@Nonnull SessionOperation<U> op) throws RepositoryException {
        return sessionDelegate.perform(op);
    }

    /**
     * Perform the passed {@link org.apache.jackrabbit.oak.jcr.ItemImpl.ItemReadOperation} assuming it does not throw an
     * {@code RepositoryException}. If it does, wrap it into and throw it as an
     * {@code IllegalArgumentException}.
     * @param op  operation to perform
     * @param <U>  return type of the operation
     * @return  the result of {@code op.perform()}
     */
    @CheckForNull
    protected final <U> U safePerform(@Nonnull SessionOperation<U> op) {
        try {
            return sessionDelegate.perform(op);
        }
        catch (RepositoryException e) {
            String msg = "Unexpected exception thrown by operation " + op;
            log.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    //---------------------------------------------------------------< Item >---

    /**
     * @see javax.jcr.Item#getName()
     */
    @Override
    @Nonnull
    public String getName() throws RepositoryException {
        return perform(new ItemReadOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                String oakName = dlg.getName();
                // special case name of root node
                return oakName.isEmpty() ? "" : toJcrPath(dlg.getName());
            }
        });
    }

    /**
     * @see javax.jcr.Property#getPath()
     */
    @Override
    @Nonnull
    public String getPath() throws RepositoryException {
        return perform(new ItemReadOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return toJcrPath(dlg.getPath());
            }
        });
    }

    @Override
    @Nonnull
    public Session getSession() throws RepositoryException {
        return sessionContext.getSession();
    }

    @Override
    public Item getAncestor(final int depth) throws RepositoryException {
        return perform(new ItemReadOperation<Item>() {
            @Override
            protected Item perform() throws RepositoryException {
                if (depth < 0) {
                    throw new ItemNotFoundException(this + ": Invalid ancestor depth (" + depth + ')');
                } else if (depth == 0) {
                    NodeDelegate nd = sessionDelegate.getRootNode();
                    if (nd == null) {
                        throw new AccessDeniedException("Root node is not accessible.");
                    }
                    return sessionContext.createNodeOrNull(nd);
                }

                String path = dlg.getPath();
                int slash = 0;
                for (int i = 0; i < depth - 1; i++) {
                    slash = PathUtils.getNextSlash(path, slash + 1);
                    if (slash == -1) {
                        throw new ItemNotFoundException(this + ": Invalid ancestor depth (" + depth + ')');
                    }
                }
                slash = PathUtils.getNextSlash(path, slash + 1);
                if (slash == -1) {
                    return ItemImpl.this;
                }

                NodeDelegate nd = sessionDelegate.getNode(path.substring(0, slash));
                if (nd == null) {
                    throw new AccessDeniedException(this + ": Ancestor access denied (" + depth + ')');
                }
                return sessionContext.createNodeOrNull(nd);
            }
        });
    }

    @Override
    public int getDepth() throws RepositoryException {
        return perform(new ItemReadOperation<Integer>() {
            @Override
            public Integer perform() throws RepositoryException {
                return PathUtils.getDepth(dlg.getPath());
            }
        });
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
     * @see javax.jcr.Item#save()
     */
    @Override
    public void save() throws RepositoryException {
        log.warn("Item#save is no longer supported. Please use Session#save instead.");
        
        if (isNew()) {
            throw new RepositoryException("Item.save() not allowed on new item");
        }
        
        getSession().save();
    }

    /**
     * @see Item#refresh(boolean)
     */
    @Override
    public void refresh(boolean keepChanges) throws RepositoryException {
        log.warn("Item#refresh is no longer supported. Please use Session#refresh");
        getSession().refresh(keepChanges);
    }

    @Override
    public String toString() {
        return (isNode() ? "Node[" : "Property[") + dlg + ']';
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Performs a sanity check on this item and the associated session.
     *
     * @throws RepositoryException if this item has been rendered invalid for some reason
     * or the associated session has been logged out.
     */
    synchronized void checkAlive() throws RepositoryException {
        sessionDelegate.checkAlive();
        long count = sessionDelegate.getUpdateCount();
        if (updateCount != count) {
            dlg.checkNotStale();
            updateCount = count;
        }
    }

    void checkProtected() throws RepositoryException {
        if (dlg.isProtected()) {
            throw new ConstraintViolationException("Item is protected.");
        }
    }

    void checkProtected(ItemDefinition definition) throws ConstraintViolationException {
        if (definition.isProtected()) {
            throw new ConstraintViolationException("Item is protected.");
        }
    }

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
    NodeTypeManager getNodeTypeManager() {
        return sessionContext.getNodeTypeManager();
    }

    @Nonnull
    DefinitionProvider getDefinitionProvider() {
        return sessionContext.getDefinitionProvider();
    }

    @Nonnull
    EffectiveNodeTypeProvider getEffectiveNodeTypeProvider() {
        return sessionContext.getEffectiveNodeTypeProvider();
    }

    @Nonnull
    VersionManager getVersionManager() throws RepositoryException {
        return sessionContext.getVersionManager();
    }

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

    protected PropertyState createMultiState(
            String oakName, List<Value> values, Type<?> type)
            throws RepositoryException {
        if (values.isEmpty()) {
            Type<?> base = type.getBaseType();
            if (base == UNDEFINED) {
                base = STRING;
            }
            return MemoryPropertyBuilder.array(base)
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
