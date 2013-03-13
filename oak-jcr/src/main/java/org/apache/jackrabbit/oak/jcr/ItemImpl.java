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

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.commons.AbstractItem;
import org.apache.jackrabbit.oak.jcr.delegate.ItemDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionOperation;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
abstract class ItemImpl<T extends ItemDelegate> extends AbstractItem {

    /**
     * Flag to disable expensive transient item definition checks.
     * FIXME: This flag should be removed once OAK-652 gets resolved.
     */
    protected static final boolean DISABLE_TRANSIENT_DEFINITION_CHECKS =
            Boolean.getBoolean("OAK-652");

    protected final SessionDelegate sessionDelegate;
    protected final T dlg;

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ItemImpl.class);

    protected ItemImpl(T itemDelegate) {
        this.sessionDelegate = itemDelegate.getSessionDelegate();
        this.dlg = itemDelegate;
    }

    protected <X> X perform(SessionOperation<X> operation)
            throws RepositoryException {
        return sessionDelegate.perform(operation);
    }

    //---------------------------------------------------------------< Item >---

    /**
     * @see javax.jcr.Item#getName()
     */
    @Override
    @Nonnull
    public String getName() throws RepositoryException {
        return perform(new SessionOperation<String>() {
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
        return perform(new SessionOperation<String>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                checkStatus();
            }

            @Override
            public String perform() throws RepositoryException {
                return toJcrPath(dlg.getPath());
            }
        });
    }

    @Override
    @Nonnull
    public Session getSession() throws RepositoryException {
        return SessionContextProvider.getSession(sessionDelegate);
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
     */
    void checkStatus() throws RepositoryException {
        if (dlg.isStale()) {
            throw new InvalidItemStateException("stale");
        }

        // check session status
        if (!sessionDelegate.isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }

        // TODO: validate item state.
    }

    public void checkProtected() throws RepositoryException {
        if (DISABLE_TRANSIENT_DEFINITION_CHECKS) {
            return;
        }

        ItemDefinition definition;
        try {
            definition = (isNode()) ? ((Node) this).getDefinition() : ((Property) this).getDefinition();
        }
        catch (RepositoryException ignore) {
            // No definition -> not protected but a different error which should be handled else where
            return;
        }
        checkProtected(definition);
    }

    void checkProtected(ItemDefinition definition) throws RepositoryException {
        if (definition.isProtected()) {
            throw new ConstraintViolationException("Item is protected.");
        }
    }

    /**
     * Ensure that the associated session has no pending changes and throw an
     * exception otherwise.
     *
     * @throws InvalidItemStateException if this nodes session has pending changes
     * @throws RepositoryException
     */
    void ensureNoPendingSessionChanges() throws RepositoryException {
        // check for pending changes
        if (sessionDelegate.hasPendingChanges()) {
            String msg = "Unable to perform operation. Session has pending changes.";
            log.debug(msg);
            throw new InvalidItemStateException(msg);
        }
    }

    /**
     * Returns the value factory associated with the editing session.
     *
     * @return the value factory
     */
    @Nonnull
    ValueFactoryImpl getValueFactory() {
        return SessionContextProvider.getValueFactory(sessionDelegate);
    }

    @Nonnull
    NodeTypeManager getNodeTypeManager() {
        return SessionContextProvider.getNodeTypeManager(sessionDelegate);
    }

    @Nonnull
    DefinitionProvider getDefinitionProvider() {
        return SessionContextProvider.getDefinitionProvider(sessionDelegate);
    }

    @Nonnull
    String toJcrPath(String oakPath) {
        NamePathMapper namePathMapper = SessionContextProvider.getNamePathMapper(sessionDelegate);
        return namePathMapper.getJcrPath(oakPath);
    }
}
