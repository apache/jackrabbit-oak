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

import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;

/**
 * Implementation of {@link NamespaceRegistry}.
 */
public abstract class NamespaceRegistryImpl
        implements NamespaceRegistry, NamespaceConstants {

    /**
     * Called by the {@link NamespaceRegistry} implementation methods
     * to acquire a root {@link Tree} instance from which to read the
     * namespace mappings (under <code>jcr:system/rep:namespaces</code>).
     *
     * @return root {@link Tree} for reading the namespace mappings
     */
    abstract protected Tree getReadTree();

    /**
     * Called by the {@link #registerNamespace(String, String)} and
     * {@link #unregisterNamespace(String)} methods to acquire a fresh
     * {@link Root} instance that can be used to persist the requested
     * namespace changes (and nothing else).
     * <p>
     * The default implementation of this method throws an
     * {@link UnsupportedOperationException}.
     *
     * @return fresh {@link Root} instance
     */
    protected Root getWriteRoot() {
        throw new UnsupportedOperationException();
    }

    /**
     * Called by the {@link NamespaceRegistry} implementation methods to
     * refresh the state of the session associated with this instance.
     * That way the session is kept in sync with the latest global state
     * seen by the namespace registry.
     *
     * @throws RepositoryException if the session could not be refreshed
     */
    protected void refresh() throws RepositoryException {
    }

    //--------------------------------------------------< NamespaceRegistry >---

    @Override
    public void registerNamespace(String prefix, String uri)
            throws RepositoryException {
        try {
            Root root = getWriteRoot();
            Tree namespaces =
                    getOrCreate(root, JcrConstants.JCR_SYSTEM, REP_NAMESPACES);
            // remove existing mapping to given uri
            for (PropertyState p : namespaces.getProperties()) {
                if (!p.isArray() && p.getValue().getString().equals(uri)) {
                    namespaces.removeProperty(p.getName());
                }
            }
            namespaces.setProperty(prefix, new StringValue(uri));
            root.commit(DefaultConflictHandler.OURS);
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
            root.commit(DefaultConflictHandler.OURS);
            refresh();
        } catch (NamespaceValidatorException e) {
            throw e.getNamespaceException();
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to unregister namespace mapping for prefix "
                    + prefix, e);
        }
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

    @Override
    @Nonnull
    public String[] getPrefixes() throws RepositoryException {
        try {
            Tree root = getReadTree();
            Map<String, String> map = Namespaces.getNamespaceMap(root);
            String[] prefixes = map.keySet().toArray(new String[map.size()]);
            Arrays.sort(prefixes);
            return prefixes;
        } catch (RuntimeException e) {
            throw new RepositoryException(
                    "Failed to retrieve registered namespace prefixes", e);
        }
    }

    @Override
    @Nonnull
    public String[] getURIs() throws RepositoryException {
        try {
            Tree root = getReadTree();
            Map<String, String> map = Namespaces.getNamespaceMap(root);
            String[] uris = map.values().toArray(new String[map.size()]);
            Arrays.sort(uris);
            return uris;
        } catch (RuntimeException e) {
            throw new RepositoryException(
                    "Failed to retrieve registered namespace URIs", e);
        }
    }

    @Override
    @Nonnull
    public String getURI(String prefix) throws RepositoryException {
        try {
            Tree root = getReadTree();
            Map<String, String> map = Namespaces.getNamespaceMap(root);
            String uri = map.get(prefix);
            if (uri == null) {
                throw new NamespaceException(
                        "No namespace registered for prefix " + prefix);
            }
            return uri;
        } catch (RuntimeException e) {
            throw new RepositoryException(
                    "Failed to retrieve the namespace URI for prefix "
                    + prefix, e);
        }
    }

    @Override
    @Nonnull
    public String getPrefix(String uri) throws RepositoryException {
        try {
            Tree root = getReadTree();
            Map<String, String> map = Namespaces.getNamespaceMap(root);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (entry.getValue().equals(uri)) {
                    return entry.getKey();
                }
            }
            throw new NamespaceException(
                        "No namespace prefix registered for URI " + uri);
        } catch (RuntimeException e) {
            throw new RepositoryException(
                    "Failed to retrieve the namespace prefix for URI "
                    + uri, e);
        }
    }

}
