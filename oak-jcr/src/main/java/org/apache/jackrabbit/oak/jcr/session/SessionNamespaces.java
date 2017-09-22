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
package org.apache.jackrabbit.oak.jcr.session;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Sets.newHashSet;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.NamespaceException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.util.XMLChar;

import com.google.common.collect.Maps;

/**
 * {@code SessionNamespaces} implements namespace handling on the JCR
 * Session level. That is, it maintains a map of session local namespace
 * re-mappings and takes a snapshot of the namespace registry when initialized
 * (see JCR 2.0 specification, section 3.5.1).
 */
public class SessionNamespaces extends LocalNameMapper {

    public SessionNamespaces(@Nonnull Root root) {
        super(root, Maps.<String, String>newHashMap());
    }

    // The code below was initially copied from JCR Commons AbstractSession,
    // but has since been radically modified

    /**
     * @see Session#setNamespacePrefix(String, String)
     */
    synchronized void setNamespacePrefix(String prefix, String uri)
            throws NamespaceException {
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

        // remove the possible existing mapping for the given prefix
        local.remove(prefix);

        // remove the possible existing mapping(s) for the given URI
        Set<String> prefixes = new HashSet<String>();
        for (Map.Entry<String, String> entry : local.entrySet()) {
            if (entry.getValue().equals(uri)) {
                prefixes.add(entry.getKey());
            }
        }
        local.keySet().removeAll(prefixes);

        // add the new mapping
        local.put(prefix, uri);
    }

    /**
     * @see Session#getNamespacePrefixes()
     */
    synchronized String[] getNamespacePrefixes() {
        // get registered namespace prefixes
        Iterable<String> global = getPrefixes();

        // unless there are local remappings just use the registered ones
        if (local.isEmpty()) {
            return toArray(global, String.class);
        }

        Set<String> prefixes = newHashSet(global);

        // remove the prefixes of the namespaces that have been remapped
        for (String uri : local.values()) {
            String prefix = getOakPrefixOrNull(uri);
            if (prefix != null) {
                prefixes.remove(prefix);
            }
        }

        // add the prefixes in local remappings
        prefixes.addAll(local.keySet());

        return prefixes.toArray(new String[prefixes.size()]);
    }

    /**
     * @see Session#getNamespaceURI(String)
     */
    synchronized String getNamespaceURI(String prefix)
            throws NamespaceException {
        // first check local remappings
        String uri = local.get(prefix);
        if (uri == null) {
            // Not in snapshot mappings, try the global ones
            uri = getOakURIOrNull(prefix);
            if (uri == null || local.containsValue(uri)) {
                // URI is either not registered or locally mapped to some
                // other prefix, so there are no mappings for this prefix
                throw new NamespaceException(
                        "Unknown namespace prefix: " + prefix);
            }
        }
        return uri;
    }

    /**
     * @see Session#getNamespacePrefix(String)
     */
    synchronized String getNamespacePrefix(String uri)
            throws NamespaceException {
        // first check local remappings
        for (Map.Entry<String, String> entry : local.entrySet()) {
            if (entry.getValue().equals(uri)) {
                return entry.getKey();
            }
        }

        // then try the global mappings
        String prefix = getOakPrefixOrNull(uri);
        if (prefix == null) {
            throw new NamespaceException("Unknown namespace URI: " + uri);
        }

        // Generate a new prefix if already locally mapped to something else
        String base = prefix;
        for (int i = 2; local.containsKey(prefix); i++) {
            prefix = base + i;
        }
        if (base != prefix) {
            local.put(prefix, uri);
        }

        return prefix;
    }

    /**
     * Clears the re-mapped namespaces map.
     */
    synchronized void clear() {
        local.clear();
    }

}
