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
package org.apache.jackrabbit.oak.jcr.namespace;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.plugins.name.NamespaceMappings;

/**
 * Implementation of {@link NamespaceRegistry} based on {@link NamespaceMappings}.
 */
public class NamespaceRegistryImpl implements NamespaceRegistry {

    private final NamespaceMappings nsMappings;

    public NamespaceRegistryImpl(ContentSession session) {
        this.nsMappings = new NamespaceMappings(session);
    }

    //--------------------------------------------------< NamespaceRegistry >---
    @Override
    public void registerNamespace(String prefix, String uri)
            throws RepositoryException {
        try {
            nsMappings.registerNamespace(prefix, uri);
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to register namespace mapping from "
                    + prefix + " to " + uri, e);
        }
    }

    @Override
    public void unregisterNamespace(String prefix) throws RepositoryException {
        try {
            nsMappings.unregisterNamespace(prefix);
        } catch (CommitFailedException e) {
            throw new RepositoryException(
                    "Failed to unregister a namespace mapping with prefix "
                    + prefix, e);
        }
    }

    @Override
    public String[] getPrefixes() throws RepositoryException {
        try {
            return nsMappings.getPrefixes();
        } catch (RuntimeException e) {
            throw new RepositoryException(
                    "Failed to retrieve registered namespace prefixes", e);
        }
    }

    @Override
    public String[] getURIs() throws RepositoryException {
        try {
            return nsMappings.getURIs();
        } catch (RuntimeException e) {
            throw new RepositoryException(
                    "Failed to retrieve registered namespace URIs", e);
        }
    }

    @Override
    public String getURI(String prefix) throws RepositoryException {
        try {
            String uri = nsMappings.getURI(prefix);
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
    public String getPrefix(String uri) throws RepositoryException {
        try {
            String prefix = nsMappings.getPrefix(uri);
            if (prefix == null) {
                throw new NamespaceException(
                        "No namespace registered for prefix " + prefix);
            }
            return prefix;
        } catch (RuntimeException e) {
            throw new RepositoryException(
                    "Failed to retrieve the namespace prefix for URI "
                    + uri, e);
        }
    }
}
