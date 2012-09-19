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
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.api.Tree;

/**
 * Read-only namespace registry. Used mostly internally when access to the
 * in-content registered namespaces is needed. See the
 * {@link ReadWriteNamespaceRegistry} subclass for a more complete registry
 * implementation that supports also namespace modifications and that's thus
 * better suited for use in in implementing the full JCR API.
 */
public abstract class ReadOnlyNamespaceRegistry
        implements NamespaceRegistry, NamespaceConstants {

    /**
     * Called by the {@link NamespaceRegistry} implementation methods
     * to acquire a root {@link Tree} instance from which to read the
     * namespace mappings (under <code>jcr:system/rep:namespaces</code>).
     *
     * @return root {@link Tree} for reading the namespace mappings
     */
    protected abstract Tree getReadTree();

    //--------------------------------------------------< NamespaceRegistry >---

    @Override
    public void registerNamespace(String prefix, String uri)
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void unregisterNamespace(String prefix) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
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
