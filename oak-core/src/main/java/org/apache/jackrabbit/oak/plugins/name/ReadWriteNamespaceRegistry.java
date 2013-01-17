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

import java.util.Map;

import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * Writable namespace registry. Mainly for use to implement the full JCR API.
 */
public abstract class ReadWriteNamespaceRegistry
        extends ReadOnlyNamespaceRegistry {

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

    private static Tree getOrCreate(Root root, String... path) {
        Tree tree = root.getTree("/");
        assert tree != null;
        for (String name : path) {
            Tree child = tree.getChild(name);
            if (child == null) {
                child = tree.addChild(name);
            }
            tree = child;
        }
        return tree;
    }

    //--------------------------------------------------< NamespaceRegistry >---

    @Override
    public void registerNamespace(String prefix, String uri)
            throws RepositoryException {
        Map<String, String> map = Namespaces.getNamespaceMap(getReadTree());
        if (uri.equals(map.get(prefix))) {
            return; // Namespace already registered, so we do nothing
        }

        try {
            Root root = getWriteRoot();
            Tree namespaces =
                    getOrCreate(root, JcrConstants.JCR_SYSTEM, REP_NAMESPACES);
            if (!namespaces.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
                namespaces.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                        JcrConstants.NT_UNSTRUCTURED, NAME);
            }
            // remove existing mapping to given uri
            for (PropertyState p : namespaces.getProperties()) {
                if (!p.isArray() && p.getValue(STRING).equals(uri)) {
                    namespaces.removeProperty(p.getName());
                }
            }
            namespaces.setProperty(prefix, uri);
            root.commit();
            refresh();
        } catch (NamespaceValidatorException e) {
            throw e.getNamespaceException();
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to register namespace mapping from "
                    + prefix + " to " + uri, e);
        }
    }

    @Override
    public void unregisterNamespace(String prefix) throws RepositoryException {
        Root root = getWriteRoot();
        Tree namespaces = root.getTree(NAMESPACES_PATH);
        if (namespaces == null || !namespaces.hasProperty(prefix)) {
            throw new NamespaceException(
                    "Namespace mapping from " + prefix + " to "
                    + getURI(prefix) + " can not be unregistered");
        }

        try {
            namespaces.removeProperty(prefix);
            root.commit();
            refresh();
        } catch (NamespaceValidatorException e) {
            throw e.getNamespaceException();
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to unregister namespace mapping for prefix "
                    + prefix, e);
        }
    }

}
