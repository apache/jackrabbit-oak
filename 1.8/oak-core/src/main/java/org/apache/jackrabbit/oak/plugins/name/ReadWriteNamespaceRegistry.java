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
package org.apache.jackrabbit.oak.plugins.name;

import static org.apache.jackrabbit.oak.api.Type.STRING;

import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;


/**
 * Writable namespace registry. Mainly for use to implement the full JCR API.
 */
public abstract class ReadWriteNamespaceRegistry
        extends ReadOnlyNamespaceRegistry {

    public ReadWriteNamespaceRegistry(Root root) {
        super(root);
    }

    /**
     * Called by the write methods to acquire a fresh {@link Root} instance
     * that can be used to persist the requested namespace changes (and
     * nothing else).
     *
     * @return fresh {@link Root} instance
     */
    protected abstract Root getWriteRoot();

    /**
     * Called by the write methods to refresh the state of the possible
     * session associated with this instance. The default implementation
     * of this method does nothing, but a subclass can use this callback
     * to keep a session in sync with the persisted namespace changes.
     *
     * @throws RepositoryException if the session could not be refreshed
     */
    protected void refresh() throws RepositoryException {
        // do nothing
    }

    //--------------------------------------------------< NamespaceRegistry >---

    @Override
    public void registerNamespace(String prefix, String uri)
            throws RepositoryException {
        if (prefix.isEmpty() && uri.isEmpty()) {
            return; // the default empty namespace is always registered
        } else if (prefix.isEmpty() || uri.isEmpty()) {
            throw new NamespaceException(
                    "Cannot remap the default empty namespace");
        }

        PropertyState property = namespaces.getProperty(prefix);
        if (property != null && property.getType() == STRING
                && uri.equals(property.getValue(STRING))) {
            return; // common case: namespace already registered -> do nothing
        }

        try {
            Root root = getWriteRoot();
            Tree namespaces = root.getTree(NAMESPACES_PATH);

            // remove existing mapping to given URI
            for (PropertyState mapping : namespaces.getProperties()) {
                if (mapping.getType() == STRING
                        && uri.equals(mapping.getValue(STRING))) {
                    namespaces.removeProperty(mapping.getName());
                }
            }

            // add this mapping (overrides existing mapping with same prefix)
            namespaces.setProperty(prefix, uri);

            root.commit();
            refresh();
        } catch (CommitFailedException e) {
            throw e.asRepositoryException(
                    "Failed to register namespace mapping "
                    + prefix + " -> " + uri);
        }
    }

    @Override
    public void unregisterNamespace(String prefix) throws RepositoryException {
        if (prefix.isEmpty()) {
            throw new NamespaceException(
                    "Cannot unregister the default empty namespace");
        }

        Root root = getWriteRoot();
        Tree namespaces = root.getTree(NAMESPACES_PATH);
        if (!namespaces.exists() || !namespaces.hasProperty(prefix)) {
            throw new NamespaceException(
                    "Namespace mapping from " + prefix + " to "
                            + getURI(prefix) + " can not be unregistered");
        }

        try {
            namespaces.removeProperty(prefix);
            root.commit();
            refresh();
        } catch (CommitFailedException e) {
            String message = "Failed to unregister namespace mapping for prefix " + prefix;
            throw e.asRepositoryException(message);
        }
    }

}
