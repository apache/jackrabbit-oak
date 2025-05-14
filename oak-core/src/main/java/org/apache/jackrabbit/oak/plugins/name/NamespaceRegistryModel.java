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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.collections.StreamUtils;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.util.Text;

import javax.jcr.RepositoryException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

public final class NamespaceRegistryModel {
    private final Set<String> registeredPrefixes;
    private final Set<String> registeredNamespacesEncoded;
    private final Map<String, String> prefixToNamespaceMap;
    private final Map<String, String> namespaceToPrefixMap;

    private Set<String> mappedPrefixes;
    private Set<String> mappedNamespaces;
    private Set<String> mappedToPrefixes;
    private Set<String> mappedToNamespacesEncoded;
    private Set<String> allPrefixes;
    private Set<String> allNamespacesEncoded;
    private Set<String> consistentPrefixes;
    private Set<String> consistentNamespaces;
    private int registrySize;

    private boolean consistent = false;
    private boolean fixable = false;

    private NamespaceRegistryModel(
            Set<String> registeredPrefixes, Set<String> registeredNamespacesEncoded,
            // prefixes to URIs
            Map<String, String> prefixToNamespaceMap,
            // encoded URIs to prefixes
            Map<String, String> namespaceToPrefixMap) {
        // ignore the empty namespace which is not mapped
        this.registeredPrefixes = registeredPrefixes.stream().filter(s -> !(Objects.isNull(s) || s.isEmpty())).collect(Collectors.toSet());
        this.registeredNamespacesEncoded = registeredNamespacesEncoded.stream().filter(s -> !(Objects.isNull(s) || s.isEmpty())).collect(Collectors.toSet());
        this.prefixToNamespaceMap = prefixToNamespaceMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.namespaceToPrefixMap = namespaceToPrefixMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        init();
    }

    private void init() {
        this.mappedPrefixes = prefixToNamespaceMap.keySet();
        this.mappedNamespaces = namespaceToPrefixMap.keySet();
        this.mappedToPrefixes = new HashSet<>(namespaceToPrefixMap.values());
        this.mappedToNamespacesEncoded = prefixToNamespaceMap.values().stream().map(Namespaces::encodeUri).collect(Collectors.toSet());
        allPrefixes = SetUtils.union(SetUtils.union(registeredPrefixes, mappedPrefixes), mappedToPrefixes);
        allNamespacesEncoded = SetUtils.union(SetUtils.union(registeredNamespacesEncoded, mappedNamespaces), mappedToNamespacesEncoded);
        registrySize = Math.max(allPrefixes.size(), allNamespacesEncoded.size());
        consistentPrefixes = SetUtils.intersection(SetUtils.intersection(registeredPrefixes, mappedPrefixes), mappedToPrefixes);
        consistentNamespaces = SetUtils.intersection(SetUtils.intersection(registeredNamespacesEncoded, mappedNamespaces), mappedToNamespacesEncoded);
        refresh();
    }

    private void refresh() {
        consistent = consistentPrefixes.size() == consistentNamespaces.size()
                && consistentPrefixes.size() == allPrefixes.size();
        fixable = consistent 
                || registrySize == SetUtils.union(mappedPrefixes, mappedToPrefixes).size()
                && registrySize == SetUtils.union(mappedNamespaces, mappedToNamespacesEncoded).size();
    }
    
    static NamespaceRegistryModel create(Tree namespaces) {
        Tree nsdata = namespaces.getChild(NamespaceConstants.REP_NSDATA);
        Map<String, String> prefixToNamespaceMap = new HashMap<>();
        Map<String, String> namespaceToPrefixMap = new HashMap<>();
        for (PropertyState propertyState : namespaces.getProperties()) {
            String prefix = propertyState.getName();
            if (!prefix.equals(NodeTypeConstants.JCR_PRIMARYTYPE)) {
                prefixToNamespaceMap.put(prefix, propertyState.getValue(STRING));
            }
        }
        for (PropertyState propertyState : nsdata.getProperties()) {
            String encodedUri = propertyState.getName();
            switch (encodedUri) {
                case NamespaceConstants.REP_PREFIXES:
                case NamespaceConstants.REP_URIS:
                case NodeTypeConstants.JCR_PRIMARYTYPE:
                    break;
                default:
                    namespaceToPrefixMap.put(encodedUri, propertyState.getValue(STRING));
            }
        }
        Iterable<String> uris = nsdata.getProperty(NamespaceConstants.REP_URIS).getValue(STRINGS);
        NamespaceRegistryModel model = new NamespaceRegistryModel(
                new HashSet<>(Arrays.asList(IterableUtils.toArray(nsdata.getProperty(NamespaceConstants.REP_PREFIXES).getValue(STRINGS), String.class))),
                StreamUtils.toStream(uris).map(Namespaces::encodeUri).collect(Collectors.toSet()),
                prefixToNamespaceMap, namespaceToPrefixMap);
        return model;
    }

    NamespaceRegistryModel tryRegistryRepair() {
        refresh();
        if (consistent) {
            return this;
        }
        if (!fixable) {
            return null;
        }
        HashSet<String> fixedRegisteredPrefixes = new HashSet<>();
        HashMap<String, String> fixedPrefixToNamespaceMap = new HashMap<>();
        for (String prefix : allPrefixes) {
            fixedRegisteredPrefixes.add(prefix);
            if (mappedPrefixes.contains(prefix)) {
                fixedPrefixToNamespaceMap.put(prefix, prefixToNamespaceMap.get(prefix));
            } else {
                for (Map.Entry<String, String> entry : namespaceToPrefixMap.entrySet()) {
                    if (entry.getValue().equals(prefix)) {
                        fixedPrefixToNamespaceMap.put(prefix, Text.unescapeIllegalJcrChars(entry.getKey()));
                        break;
                    }
                }
            }
        }
        HashSet<String> fixedRegisteredNamespacesEncoded = new HashSet<>();
        HashMap<String, String> fixedNamespaceToPrefixMap = new HashMap<>();
        for (String encodedNamespace : allNamespacesEncoded) {
            fixedRegisteredNamespacesEncoded.add(encodedNamespace);
            if (mappedNamespaces.contains(encodedNamespace)) {
                fixedNamespaceToPrefixMap.put(encodedNamespace, namespaceToPrefixMap.get(encodedNamespace));
            } else {
                for (Map.Entry<String, String> entry : prefixToNamespaceMap.entrySet()) {
                    if (Namespaces.encodeUri(entry.getValue()).equals(encodedNamespace)) {
                        fixedNamespaceToPrefixMap.put(encodedNamespace, entry.getKey());
                        break;
                    }
                }
            }
        }
        return new NamespaceRegistryModel(fixedRegisteredPrefixes, fixedRegisteredNamespacesEncoded,
                fixedPrefixToNamespaceMap, fixedNamespaceToPrefixMap);
    }

    public void apply(Tree namespaces) throws RepositoryException, CommitFailedException {
        Tree nsdata = namespaces.getChild(NamespaceConstants.REP_NSDATA);
        for (PropertyState propertyState : namespaces.getProperties()) {
            String name = propertyState.getName();
            if (!JCR_PRIMARYTYPE.equals(name)) {
                namespaces.removeProperty(name);
            }
        }
        for (Map.Entry<String, String> entry : prefixToNamespaceMap.entrySet()) {
            String prefix = entry.getKey();
            String uri = entry.getValue();
            namespaces.setProperty(prefix, uri);
        }
        for (PropertyState propertyState : nsdata.getProperties()) {
            String name = propertyState.getName();
            if (!JCR_PRIMARYTYPE.equals(name)) {
                nsdata.removeProperty(name);
            }
        }
        for (Map.Entry<String, String> entry : namespaceToPrefixMap.entrySet()) {
            String encodedUri = entry.getKey();
            String prefix = entry.getValue();
            nsdata.setProperty(encodedUri, prefix);
        }
        nsdata.setProperty(NamespaceConstants.REP_PREFIXES, mappedPrefixes, STRINGS);
        nsdata.setProperty(NamespaceConstants.REP_URIS, prefixToNamespaceMap.values(), STRINGS);
        refresh();
        if (!consistent) {
            throw new IllegalStateException("Final registry consistency check failed.");
        }
    }

    public boolean isConsistent() {
        return consistent;
    }

    public boolean isFixable() {
        return fixable;
    }
}
