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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkState;
import static javax.jcr.NamespaceRegistry.NAMESPACE_EMPTY;
import static javax.jcr.NamespaceRegistry.NAMESPACE_JCR;
import static javax.jcr.NamespaceRegistry.NAMESPACE_MIX;
import static javax.jcr.NamespaceRegistry.NAMESPACE_NT;
import static javax.jcr.NamespaceRegistry.NAMESPACE_XML;
import static javax.jcr.NamespaceRegistry.PREFIX_EMPTY;
import static javax.jcr.NamespaceRegistry.PREFIX_JCR;
import static javax.jcr.NamespaceRegistry.PREFIX_MIX;
import static javax.jcr.NamespaceRegistry.PREFIX_NT;
import static javax.jcr.NamespaceRegistry.PREFIX_XML;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * Internal static utility class for managing the persisted namespace registry.
 */
public class Namespaces implements NamespaceConstants {

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
        namespaces.setProperty(escapePropertyKey(PREFIX_EMPTY), NAMESPACE_EMPTY);
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
                String prefix = unescapePropertyKey(property.getName());
                if (isValidPrefix(prefix)
                        && uri.equals(property.getValue(STRING))) {
                    return prefix;
                }
            }
        }

        // no existing mapping found for the URI, make sure prefix is unique
        String prefix = prefixHint;
        int iteration = 1;
        while (namespaces.hasProperty(escapePropertyKey(prefix))) {
            prefix = prefixHint + ++iteration;
        }

        // add the new mapping with its unique prefix
        namespaces.setProperty(escapePropertyKey(prefix), uri);
        return prefix;
    }

    public static void buildIndexNode(NodeBuilder namespaces) {
        Set<String> prefixes = new HashSet<String>();
        Set<String> uris = new HashSet<String>();
        Map<String, String> reverse = new HashMap<String, String>();

        for (PropertyState property : namespaces.getProperties()) {
            String prefix = unescapePropertyKey(property.getName());
            if (STRING.equals(property.getType()) && isValidPrefix(prefix)) {
                prefixes.add(prefix);
                String uri = property.getValue(STRING);
                uris.add(uri);
                reverse.put(escapePropertyKey(uri), prefix);
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
        Map<String, String> map = new HashMap<String, String>();

        Tree namespaces = getNamespaceTree(root);
        for (PropertyState property : namespaces.getProperties()) {
            String prefix = property.getName();
            if (STRING.equals(property.getType()) && isValidPrefix(prefix)) {
                map.put(unescapePropertyKey(prefix), property.getValue(STRING));
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
        Tree namespaces = getNamespaceTree(root);
        PropertyState ps = namespaces.getChild(REP_NSDATA)
                .getProperty(encodeUri(escapePropertyKey(uri)));
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
        if (isValidPrefix(prefix)) {
            PropertyState property = getNamespaceTree(root).getProperty(
                    escapePropertyKey(prefix));
            if (property != null && STRING.equals(property.getType())) {
                return property.getValue(STRING);
            }
        }
        return null;
    }

    // utils

    /**
     * Replaces an empty string with the special {@link #REP_EMPTY} value.
     *
     * @see #unescapePropertyKey(String)
     * @param key property key
     * @return escaped property key
     */
    static String escapePropertyKey(String key) {
        if (key.equals("")) {
            return REP_EMPTY;
        } else {
            return key;
        }
    }

    /**
     * Converts the special {@link #REP_EMPTY} value back to an empty string.
     *
     * @see #escapePropertyKey(String)
     * @param key property key
     * @return escaped property key
     */
    static String unescapePropertyKey(String key) {
        if (key.equals(REP_EMPTY)) {
            return "";
        } else {
            return key;
        }
    }

    /**
     * encodes the uri value to be used as a property
     * 
     * @param uri
     * @return encoded uri
     */
    static String encodeUri(String uri) {
        return Text.escapeIllegalJcrChars(uri);
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
            if ("/:[]|*".indexOf(ch) != -1) { // TODO: XMLChar check
                return false;
            }
        }

        // TODO: Other name rules?
        return true;
    }

    // testing

    public static Tree setupTestNamespaces(Map<String, String> global) {
        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder namespaces = root.child(JCR_SYSTEM).child(REP_NAMESPACES);
        namespaces.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_UNSTRUCTURED, NAME);
        for (Entry<String, String> entry : global.entrySet()) {
            namespaces.setProperty(escapePropertyKey(entry.getKey()),
                    entry.getValue());
        }
        buildIndexNode(namespaces);
        return new ImmutableTree(root.getNodeState());
    }

}
