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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static javax.jcr.NamespaceRegistry.NAMESPACE_JCR;
import static javax.jcr.NamespaceRegistry.NAMESPACE_MIX;
import static javax.jcr.NamespaceRegistry.NAMESPACE_NT;
import static javax.jcr.NamespaceRegistry.NAMESPACE_XML;
import static javax.jcr.NamespaceRegistry.PREFIX_JCR;
import static javax.jcr.NamespaceRegistry.PREFIX_MIX;
import static javax.jcr.NamespaceRegistry.PREFIX_NT;
import static javax.jcr.NamespaceRegistry.PREFIX_XML;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.util.Text.escapeIllegalJcrChars;

/**
 * Internal static utility class for managing the persisted namespace registry.
 */
public class Namespaces implements NamespaceConstants {

    /**
     * Global cache of encoded URIs.
     */
    private static final Map<String, String> ENCODED_URIS = newConcurrentMap();

    /**
     * By default node names with non space whitespace chars are not allowed.
     * However initial Oak release did allowed that and this flag is provided
     * to revert back to old behaviour if required for some case temporarily
     */
    private static final boolean allowOtherWhitespaceChars = Boolean.getBoolean("oak.allowOtherWhitespaceChars");

    private Namespaces() {
    }

    public static void setupNamespaces(NodeBuilder system) {
        if (!system.hasChildNode(REP_NAMESPACES)) {
            NodeBuilder namespaces = createStandardMappings(system);
            buildIndexNode(namespaces); // index node for faster lookup
        }
    }

    public static NodeBuilder createStandardMappings(NodeBuilder system) {
        checkState(!system.hasChildNode(REP_NAMESPACES));

        NodeBuilder namespaces = system.setChildNode(REP_NAMESPACES);
        namespaces.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_UNSTRUCTURED, NAME);

        // Standard namespace specified by JCR (default one not included)
        namespaces.setProperty(PREFIX_JCR, NAMESPACE_JCR);
        namespaces.setProperty(PREFIX_NT,  NAMESPACE_NT);
        namespaces.setProperty(PREFIX_MIX, NAMESPACE_MIX);
        namespaces.setProperty(PREFIX_XML, NAMESPACE_XML);

        // Namespace included in Jackrabbit 2.x
        namespaces.setProperty(PREFIX_SV, NAMESPACE_SV);
        namespaces.setProperty(PREFIX_REP, NAMESPACE_REP);

        return namespaces;
    }

    public static String addCustomMapping(
            NodeBuilder namespaces, String uri, String prefixHint) {
        // first look for an existing mapping for the given URI
        for (PropertyState property : namespaces.getProperties()) {
            if (property.getType() == STRING) {
                String prefix = property.getName();
                if (isValidPrefix(prefix)
                        && uri.equals(property.getValue(STRING))) {
                    return prefix;
                }
            }
        }

        // no existing mapping found for the URI, make sure prefix is unique
        String prefix = prefixHint;
        int iteration = 1;
        while (namespaces.hasProperty(prefix)) {
            prefix = prefixHint + ++iteration;
        }

        // add the new mapping with its unique prefix
        namespaces.setProperty(prefix, uri);
        return prefix;
    }

    public static void buildIndexNode(NodeBuilder namespaces) {
        // initialize prefix and URI sets with the defaults namespace
        // that's not stored along with the other mappings
        Set<String> prefixes = newHashSet("");
        Set<String> uris = newHashSet("");
        Map<String, String> reverse = new HashMap<String, String>();

        for (PropertyState property : namespaces.getProperties()) {
            String prefix = property.getName();
            if (STRING.equals(property.getType()) && isValidPrefix(prefix)) {
                prefixes.add(prefix);
                String uri = property.getValue(STRING);
                uris.add(uri);
                reverse.put(uri, prefix);
            }
        }

        NodeBuilder data = namespaces.setChildNode(REP_NSDATA);
        data.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_UNSTRUCTURED, Type.NAME);
        data.setProperty(REP_PREFIXES, prefixes, Type.STRINGS);
        data.setProperty(REP_URIS, uris, Type.STRINGS);
        for (Entry<String, String> e : reverse.entrySet()) {
            data.setProperty(encodeUri(e.getKey()), e.getValue());
        }
    }

    private static Tree getNamespaceTree(Tree root) {
        return root.getChild(JCR_SYSTEM).getChild(REP_NAMESPACES);
    }

    public static Map<String, String> getNamespaceMap(Tree root) {
        Map<String, String> map = newHashMap();
        map.put("", ""); // default namespace, not included in tree

        Tree namespaces = getNamespaceTree(root);
        for (PropertyState property : namespaces.getProperties()) {
            String prefix = property.getName();
            if (STRING.equals(property.getType()) && isValidPrefix(prefix)) {
                map.put(prefix, property.getValue(STRING));
            }
        }

        return map;
    }

    static String[] getNamespacePrefixes(Tree root) {
        Set<String> prefSet = getNamespacePrefixesAsSet(root);
        String[] prefixes = prefSet.toArray(new String[prefSet.size()]);
        Arrays.sort(prefixes);
        return prefixes;
    }

    static Set<String> getNamespacePrefixesAsSet(Tree root) {
        return safeGet(getNamespaceTree(root).getChild(REP_NSDATA), REP_PREFIXES);
    }

    public static String getNamespacePrefix(Tree root, String uri) {
        if (uri.isEmpty()) {
            return uri;
        }

        Tree nsdata = getNamespaceTree(root).getChild(REP_NSDATA);
        PropertyState ps = nsdata.getProperty(encodeUri(uri));
        if (ps != null) {
            return ps.getValue(STRING);
        }

        return null;
    }

    static String[] getNamespaceURIs(Tree root) {
        Set<String> uris = safeGet(getNamespaceTree(root).getChild(REP_NSDATA), REP_URIS);
        return uris.toArray(new String[uris.size()]);
    }

    public static String getNamespaceURI(Tree root, String prefix) {
        if (prefix.isEmpty()) {
            return prefix;
        }

        if (isValidPrefix(prefix)) {
            PropertyState property = getNamespaceTree(root).getProperty(prefix);
            if (property != null && property.getType() == STRING) {
                return property.getValue(STRING);
            }
        }

        return null;
    }

    // utils

    /**
     * encodes the uri value to be used as a property
     * 
     * @param uri
     * @return encoded uri
     */
    public static String encodeUri(String uri) {
        String encoded = ENCODED_URIS.get(uri);
        if (encoded == null) {
            encoded =  escapeIllegalJcrChars(uri);
            if (ENCODED_URIS.size() > 1000) {
                ENCODED_URIS.clear(); // prevents DoS attacks
            }
            ENCODED_URIS.put(uri, encoded);
        }
        return encoded;
    }

    static Set<String> safeGet(Tree tree, String name) {
        PropertyState ps = tree.getProperty(name);
        if (ps == null) {
            return Sets.newHashSet();
        }
        return Sets.newHashSet(ps.getValue(Type.STRINGS));
    }

    // validation

    public static boolean isValidPrefix(String prefix) {
        // TODO: Other prefix rules?
        return prefix.indexOf(':') == -1;
    }

    public static boolean isValidLocalName(String local) {
        if (local.isEmpty() || ".".equals(local) || "..".equals(local)) {
            return false;
        }

        for (int i = 0; i < local.length(); i++) {
            char ch = local.charAt(i);
            boolean spaceChar = allowOtherWhitespaceChars ? Character.isSpaceChar(ch) : Character.isWhitespace(ch);
            if (spaceChar) {
                if (i == 0) {
                    return false; // leading whitespace
                } else if (i == local.length() - 1) {
                    return false; // trailing whitespace
                } else if (ch != ' ') {
                    return false; // only spaces are allowed as whitespace
                }
            } else if ("/:[]|*".indexOf(ch) != -1) { // TODO: XMLChar check
                return false; // invalid name character
            }
        }

        // TODO: Other name rules?
        return true;
    }

}
