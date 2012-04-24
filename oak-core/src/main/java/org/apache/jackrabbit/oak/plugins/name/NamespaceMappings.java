/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.jackrabbit.oak.plugins.name;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

import javax.jcr.RepositoryException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Prove of concept implementation for OAK-61.
 *
 * For each registered mapping from a jcr prefix to a namespace a
 * a mk prefix is generated. The mk prefixes are in one to one relation
 * ship with the registered namespaces and should be used as shorthands
 * in place of the actual namespaces in all further name and path handling.
 *
 * TODO: expose the relevant methods through the Oak API.
 */
public class NamespaceMappings {

    private static final Map<String, String> defaults =
            new HashMap<String, String>();

    static {
        // Standard namespace specified by JCR (default one not included)
        defaults.put("", "");
        defaults.put("jcr", "http://www.jcp.org/jcr/1.0");
        defaults.put("nt",  "http://www.jcp.org/jcr/nt/1.0");
        defaults.put("mix", "http://www.jcp.org/jcr/mix/1.0");
        defaults.put("xml", "http://www.w3.org/XML/1998/namespace");

        // Namespace included in Jackrabbit 2.x
        defaults.put("sv", "http://www.jcp.org/jcr/sv/1.0");
        defaults.put("rep", "internal");
    }

    private final ContentSession session;

    public NamespaceMappings(ContentSession session) {
        this.session = session;
    }

    /**
     * Returns all registered namespace prefixes.
     *
     * @return newly allocated and sorted array of namespace prefixes
     */
    public String[] getPrefixes() {
        Set<String> prefixes = new HashSet<String>();
        prefixes.addAll(defaults.keySet());

        Tree namespaces = getNamespaces(session.getCurrentRoot(), false);
        if (namespaces != null) {
            for (PropertyState property : namespaces.getProperties()) {
                prefixes.add(property.getName());
            }
        }

        String[] array = prefixes.toArray(new String[prefixes.size()]);
        Arrays.sort(array);
        return array;
    }

    /**
     * Returns all registered namespace URIs.
     *
     * @return newly allocated and sorted array of namespace URIs
     */
    public String[] getURIs() {
        Set<String> uris = new HashSet<String>();
        uris.addAll(defaults.values());

        Tree namespaces = getNamespaces(session.getCurrentRoot(), false);
        if (namespaces != null) {
            for (PropertyState property : namespaces.getProperties()) {
                uris.add(property.getValue().getString());
            }
        }

        String[] array = uris.toArray(new String[uris.size()]);
        Arrays.sort(array);
        return array;
    }

    /**
     * Returns the namespace URI associated with the given prefix,
     * or {@code null} if such a mapping does not exist.
     *
     * @param prefix prefix for which to lookup the namespace URI
     * @return matching namespace prefix, or {@code null}
     */
    public String getURI(String prefix) {
        String uri = defaults.get(prefix);
        if (uri != null) {
            return uri;
        }

        Tree namespaces = getNamespaces(session.getCurrentRoot(), false);
        if (namespaces != null) {
            PropertyState property = namespaces.getProperty(prefix);
            if (property != null) {
                return property.getValue().getString();
            }
        }

        return null;
    }

    /**
     * Returns the namespace prefix associated with the given URI,
     * or {@code null} if such a mapping does not exist.
     *
     * @param uri  uri for which to lookup the prefix
     * @return matching namespace URI, or {@code null}
     */
    public String getPrefix(String uri) throws RepositoryException {
        for (Map.Entry<String, String> entry : defaults.entrySet()) {
            if (uri.equals(entry.getValue())) {
                return entry.getKey();
            }
        }

        Tree namespaces = getNamespaces(session.getCurrentRoot(), false);
        if (namespaces != null) {
            for (PropertyState property : namespaces.getProperties()) {
                if (uri.equals(property.getValue().getString())) {
                    return property.getName();
                }
            }
        }

        return null;
    }

    /**
     * Adds the specified namespace mapping.
     *
     * @param prefix namespace prefix
     * @param uri namespace URI
     * @throws CommitFailedException if the registration failed
     */
    public void registerNamespace(String prefix, String uri)
            throws CommitFailedException {
        Root root = session.getCurrentRoot();
        Tree namespaces = getNamespaces(root, true);
        namespaces.setProperty(
                prefix, session.getCoreValueFactory().createValue(uri));
        root.commit();
    }

    /**
     * Removes the namespace mapping for the given prefix.
     *
     * @param prefix namespace prefix
     * @throws CommitFailedException if the unregistering failed
     */
    public void unregisterNamespace(String prefix)
            throws CommitFailedException {
        Root root = session.getCurrentRoot();
        Tree namespaces = getNamespaces(root, true);
        namespaces.removeProperty(prefix);
        root.commit();
    }

    private static Tree getNamespaces(Root root, boolean create) {
        Tree tree = root.getTree("/");
        Tree system = tree.getChild("jcr:system");
        if (system == null) {
            if (create) {
                system = tree.addChild("jcr:system");
            } else {
                return null;
            }
        }
        Tree namespaces = system.getChild("jcr:namespaces");
        if (namespaces == null && create) {
            namespaces = system.addChild("jcr:namespaces");
        }
        return namespaces;
    }

}
