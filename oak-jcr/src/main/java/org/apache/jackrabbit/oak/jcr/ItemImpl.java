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

import org.apache.jackrabbit.oak.jcr.SessionImpl.Context;
import org.apache.jackrabbit.oak.jcr.state.TransientNodeState;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.AccessDeniedException;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Item;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.version.VersionException;

/**
 * {@code ItemImpl}...
 */
abstract class ItemImpl implements Item {
    protected final Context sessionContext;

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ItemImpl.class);

    protected ItemImpl(Context sessionContext) {
        this.sessionContext = sessionContext;
    }

    //---------------------------------------------------------------< Item >---
    @Override
    public Session getSession() throws RepositoryException {
        return sessionContext.getSession();
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
    public void save() throws AccessDeniedException, ItemExistsException, ConstraintViolationException, InvalidItemStateException, ReferentialIntegrityException, VersionException, LockException, NoSuchNodeTypeException, RepositoryException {
        throw new UnsupportedRepositoryOperationException("Use Session#save");
    }

    /**
     * @see Item#refresh(boolean)
     */
    @Override
    public void refresh(boolean keepChanges) throws InvalidItemStateException, RepositoryException {
        throw new UnsupportedRepositoryOperationException("Use Session#refresh");
    }

    //--------------------------------------------------------------------------
    /**
     * Performs a sanity check on this item and the associated session.
     *
     * @throws RepositoryException if this item has been rendered invalid for some reason
     */
    void checkStatus() throws RepositoryException {
        // check session status
        sessionContext.getSession().checkIsAlive();

        // TODO: validate item state.
    }

    /**
     * Checks if the associated session has pending changes.
     *
     * @throws InvalidItemStateException if this nodes session has pending changes
     * @throws RepositoryException
     */
    void checkSessionHasPendingChanges() throws RepositoryException {
        sessionContext.getSession().checkHasPendingChanges();
    }

    /**
     * Returns the value factory associated with the editing session.
     *
     * @return the value factory
     * @throws RepositoryException
     */
    ValueFactory getValueFactory() throws RepositoryException {
        return sessionContext.getValueFactory();
    }

    protected static TransientNodeState getNodeState(Context sessionContext, Path path) {
        return sessionContext.getNodeStateProvider().getNodeState(path);
    }

}