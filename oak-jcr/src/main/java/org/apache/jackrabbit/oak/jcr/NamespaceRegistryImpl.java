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

import org.apache.jackrabbit.oak.namepath.NamespaceMappings;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

/**
 * A naive implementation of {@link NamespaceRegistry}, hard-wiring the
 * predefined namespaces for now.
 * TODO use API only
 */
public class NamespaceRegistryImpl implements NamespaceRegistry {
    NamespaceMappings nsMappings = new NamespaceMappings();
    
    public NamespaceRegistryImpl() {
        nsMappings.registerNamespace(PREFIX_EMPTY, NAMESPACE_EMPTY);
        nsMappings.registerNamespace(PREFIX_JCR, NAMESPACE_JCR);
        nsMappings.registerNamespace(PREFIX_MIX, NAMESPACE_MIX);
        nsMappings.registerNamespace(PREFIX_NT, NAMESPACE_NT);
        nsMappings.registerNamespace(PREFIX_XML, NAMESPACE_XML);
        nsMappings.registerNamespace("sv", "http://www.jcp.org/jcr/sv/1.0");
    }

    @Override
    public void registerNamespace(String prefix, String uri) throws RepositoryException {
        nsMappings.registerNamespace(prefix, uri);
    }

    @Override
    public void unregisterNamespace(String prefix) throws RepositoryException {
        nsMappings.unregisterJcrPrefix(prefix);
    }

    @Override
    public String[] getPrefixes() throws RepositoryException {
        return nsMappings.getJcrPrefixes();
    }

    @Override
    public String[] getURIs() throws RepositoryException {
        return nsMappings.getNamespaces();
    }

    @Override
    public String getURI(String prefix) throws RepositoryException {
        String result = nsMappings.getNamespace(prefix);
        if (result == null) {
            throw new NamespaceException();
        }
        return result;
    }

    @Override
    public String getPrefix(String uri) throws RepositoryException {
        String result = nsMappings.getJcrPrefix(uri);
        if (result == null) {
            throw new NamespaceException();
        }
        return result;
    }
}
