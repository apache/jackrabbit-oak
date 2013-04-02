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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.util.XMLChar;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>SessionNamespaces</code> implements namespace handling on the JCR
 * Session level. That is, it maintains a map of session local namespace
 * re-mappings and takes a snapshot of the namespace registry when initialized
 * (see JCR 2.0 specification, section 3.5.1).
 */
class SessionNamespaces {

    /**
     * Local namespace remappings. Prefixes as keys and namespace URIs as values.
     * <p/>
     * This map is only accessed from synchronized methods (see
     * <a href="https://issues.apache.org/jira/browse/JCR-1793">JCR-1793</a>).
     */
    private final Map<String, String> namespaces;

    /**
     * A snapshot of the namespace registry when these SessionNamespaces
     * are first accessed.
     */
    private Map<String, String> snapshot;

    private final SessionContext sessionContext;

    SessionNamespaces(@Nonnull Map<String, String> namespaces,
                      @Nonnull SessionContext sessionContext) {
        this.namespaces = checkNotNull(namespaces);
        this.sessionContext = checkNotNull(sessionContext);
    }

    // The code below was initially copied from JCR Commons AbstractSession, but
    // provides information the "hasRemappings" information

    /**
     * @see Session#setNamespacePrefix(String, String)
     */
    void setNamespacePrefix(String prefix, String uri) throws RepositoryException {
        init();
        if (prefix == null) {
            throw new IllegalArgumentException("Prefix must not be null");
        } else if (uri == null) {
            throw new IllegalArgumentException("Namespace must not be null");
        } else if (prefix.isEmpty()) {
            throw new NamespaceException(
                    "Empty prefix is reserved and can not be remapped");
        } else if (uri.isEmpty()) {
            throw new NamespaceException(
                    "Default namespace is reserved and can not be remapped");
        } else if (prefix.toLowerCase(Locale.ENGLISH).startsWith("xml")) {
            throw new NamespaceException(
                    "XML prefixes are reserved: " + prefix);
        } else if (!XMLChar.isValidNCName(prefix)) {
            throw new NamespaceException(
                    "Prefix is not a valid XML NCName: " + prefix);
        }

        synchronized (namespaces) {
            // Remove existing mapping for the given prefix
            namespaces.remove(prefix);

            // Remove existing mapping(s) for the given URI
            Set<String> prefixes = new HashSet<String>();
            for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                if (entry.getValue().equals(uri)) {
                    prefixes.add(entry.getKey());
                }
            }
            namespaces.keySet().removeAll(prefixes);

            // Add the new mapping
            namespaces.put(prefix, uri);
        }
    }

    /**
     * @see Session#getNamespacePrefixes()
     */
    String[] getNamespacePrefixes() throws RepositoryException {
        init();
        synchronized (namespaces) {
            if (namespaces.isEmpty()) {
                Set<String> prefixes = snapshot.keySet();
                return prefixes.toArray(new String[prefixes.size()]);
            }
        }
        Set<String> uris = new HashSet<String>();
        uris.addAll(snapshot.values());
        synchronized (namespaces) {
            // Add namespace uris only visible to session
            uris.addAll(namespaces.values());
        }
        Set<String> prefixes = new HashSet<String>();
        for (String uri : uris) {
            prefixes.add(getNamespacePrefix(uri));
        }
        return prefixes.toArray(new String[prefixes.size()]);
    }

    /**
     * @see Session#getNamespaceURI(String)
     */
    String getNamespaceURI(String prefix) throws RepositoryException {
        init();
        synchronized (namespaces) {
            String uri = namespaces.get(prefix);

            if (uri == null) {
                // Not in local mappings, try snapshot ones
                uri = snapshot.get(prefix);
                if (uri == null) {
                    // Not in snapshot mappings, try the global ones
                    uri = getNamespaceRegistry().getURI(prefix);
                    if (namespaces.containsValue(uri)) {
                        // The global URI is locally mapped to some other prefix,
                        // so there are no mappings for this prefix
                        throw new NamespaceException("Namespace not found: " + prefix);
                    }
                }
            }

            return uri;
        }
    }

    /**
     * @see Session#getNamespacePrefix(String)
     */
    String getNamespacePrefix(String uri) throws RepositoryException {
        init();
        synchronized (namespaces) {
            for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                if (entry.getValue().equals(uri)) {
                    return entry.getKey();
                }
            }

            // try snapshot
            for (Map.Entry<String, String> entry : snapshot.entrySet()) {
                if (entry.getValue().equals(uri)) {
                    return entry.getKey();
                }
            }

            // The following throws an exception if the URI is not found, that's OK
            String prefix = getNamespaceRegistry().getPrefix(uri);

            // Generate a new prefix if the global mapping is already taken
            String base = prefix;
            for (int i = 2; namespaces.containsKey(prefix); i++) {
                prefix = base + i;
            }

            if (!base.equals(prefix)) {
                namespaces.put(prefix, uri);
            }
            return prefix;
        }
    }

    /**
     * Clears the re-mapped namespaces map.
     */
    void clear() {
        namespaces.clear();
    }

    private NamespaceRegistry getNamespaceRegistry()
            throws RepositoryException {
        return sessionContext.getWorkspace().getNamespaceRegistry();
    }

    private void init() throws RepositoryException {
        if (snapshot == null) {
            NamespaceRegistry registry = getNamespaceRegistry();
            Map<String, String> map = new HashMap<String, String>();
            for (String prefix : registry.getPrefixes()) {
                map.put(prefix, registry.getURI(prefix));
            }
            snapshot = map;
        }
    }
}
