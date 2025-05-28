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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.jcr.RepositoryException;
import org.apache.jackrabbit.util.Text;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.collections.StreamUtils;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

public final class NamespaceRegistryModel {

    private final Map<String, String> prefixToNamespaceMap;
    private final Map<String, String> namespaceToPrefixMap;

    private final Set<String> registeredPrefixes;
    private final Set<String> registeredNamespacesEncoded;
    private final Set<String> mappedPrefixes;
    private final Set<String> mappedNamespaces;
    private final Set<String> mappedToPrefixes;
    private final Set<String> mappedToNamespacesEncoded;
    private final Set<String> allPrefixes;
    private final Set<String> allNamespacesEncoded;
    private final Set<String> consistentPrefixes;
    private final Set<String> consistentNamespacesEncoded;
    private final int registrySize;

    private final Set<String> duplicatePrefixes;
    private final Set<String> duplicateNamespacesEncoded;

    private final Set<String> danglingPrefixes;
    private final Set<String> danglingNamespacesEncoded;

    private boolean consistent = false;
    private boolean fixable = false;

    private NamespaceRegistryModel(
            List<String> registeredPrefixesList, List<String> registeredNamespacesEncodedList,
            // prefixes to URIs
            Map<String, String> prefixToNamespaceMap,
            // encoded URIs to prefixes
            Map<String, String> namespaceToPrefixMap) {
        // ignore the empty namespace which is not mapped
        registeredPrefixes = registeredPrefixesList.stream().filter(s -> !(Objects.isNull(s) || s.isEmpty())).collect(Collectors.toSet());
        duplicatePrefixes = findDuplicates(registeredPrefixesList);
        registeredNamespacesEncoded = registeredNamespacesEncodedList.stream().filter(s -> !(Objects.isNull(s) || s.isEmpty())).collect(Collectors.toSet());
        duplicateNamespacesEncoded = findDuplicates(registeredNamespacesEncodedList);
        this.prefixToNamespaceMap = prefixToNamespaceMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.namespaceToPrefixMap = namespaceToPrefixMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        mappedPrefixes = prefixToNamespaceMap.keySet();
        mappedNamespaces = namespaceToPrefixMap.keySet();
        mappedToPrefixes = new HashSet<>(namespaceToPrefixMap.values());
        mappedToNamespacesEncoded = prefixToNamespaceMap.values().stream().map(Namespaces::encodeUri).collect(Collectors.toSet());
        allPrefixes = SetUtils.union(SetUtils.union(registeredPrefixes, mappedPrefixes), mappedToPrefixes);
        allNamespacesEncoded = SetUtils.union(SetUtils.union(registeredNamespacesEncoded, mappedNamespaces), mappedToNamespacesEncoded);
        registrySize = Math.max(allPrefixes.size(), allNamespacesEncoded.size());
        consistentPrefixes = SetUtils.intersection(SetUtils.intersection(registeredPrefixes, mappedPrefixes), mappedToPrefixes);
        consistentNamespacesEncoded = SetUtils.intersection(SetUtils.intersection(registeredNamespacesEncoded, mappedNamespaces), mappedToNamespacesEncoded);
        danglingPrefixes = SetUtils.difference(registeredPrefixes, SetUtils.union(mappedPrefixes, mappedToPrefixes));
        danglingNamespacesEncoded = SetUtils.difference(registeredNamespacesEncoded, SetUtils.union(mappedNamespaces, mappedToNamespacesEncoded));;
        refresh();
    }

    private void refresh() {
        consistent = duplicatePrefixes.isEmpty()
                && duplicateNamespacesEncoded.isEmpty()
                && consistentNamespacesEncoded.size() == allNamespacesEncoded.size()
                && consistentPrefixes.size() == allPrefixes.size();
        boolean roundtrips = true;
        for (String prefix : mappedPrefixes) {
            String revMapped = namespaceToPrefixMap.get(Namespaces.encodeUri(prefixToNamespaceMap.get(prefix)));
            if (revMapped != null && !revMapped.equals(prefix)) {
                roundtrips = false;
                break;
            }
        }
        if (roundtrips) {
            for (String ns : mappedNamespaces) {
                String revMapped = prefixToNamespaceMap.get(namespaceToPrefixMap.get(Namespaces.encodeUri(ns)));
                if (revMapped != null && !revMapped.equals(ns)) {
                    roundtrips = false;
                    break;
                }
            }
        }
        consistent &= roundtrips;
        fixable = consistent;
        if (!consistent && roundtrips) {
            fixable = registrySize == SetUtils.union(mappedPrefixes, mappedToPrefixes).size() 
                    && registrySize == SetUtils.union(mappedNamespaces, mappedToNamespacesEncoded).size();
        }
    }

    public static @NotNull NamespaceRegistryModel create(@NotNull Tree namespaces) {
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
        Iterable<String> uris = Objects.requireNonNull(nsdata.getProperty(NamespaceConstants.REP_URIS))
                .getValue(STRINGS);
        return new NamespaceRegistryModel(
                Arrays.asList(IterableUtils.toArray(Objects.requireNonNull(nsdata.getProperty(NamespaceConstants.REP_PREFIXES)).getValue(STRINGS), String.class)),
                StreamUtils.toStream(uris).map(Namespaces::encodeUri).collect(Collectors.toList()),
                prefixToNamespaceMap, namespaceToPrefixMap);
    }

    public NamespaceRegistryModel tryRegistryRepair() {
        return tryRegistryRepair(Collections.emptyMap());
    }

    //additional prefix to unencoded uri mappings
    public NamespaceRegistryModel tryRegistryRepair(@NotNull Map<String, String> additionalPrefixToUrisMappings) {
        List<String> fixedRegisteredPrefixesList = new ArrayList<>();
        HashMap<String, String> fixedPrefixToNamespaceMap = new HashMap<>();
        for (String prefix : allPrefixes) {
            fixedRegisteredPrefixesList.add(prefix);
            if (mappedPrefixes.contains(prefix)) {
                fixedPrefixToNamespaceMap.put(prefix, prefixToNamespaceMap.get(prefix));
            } else {
                for (Map.Entry<String, String> entry : namespaceToPrefixMap.entrySet()) {
                    if (entry.getValue().equals(prefix)) {
                        fixedPrefixToNamespaceMap.put(prefix, Text.unescape(entry.getKey()));
                        break;
                    }
                }
            }
        }
        for (String prefix : additionalPrefixToUrisMappings.keySet()) {
            fixedRegisteredPrefixesList.add(prefix);
            fixedPrefixToNamespaceMap.put(prefix, additionalPrefixToUrisMappings.get(prefix));
        }
        List<String> fixedRegisteredNamespacesEncodedList = new ArrayList<>();
        HashMap<String, String> fixedNamespaceToPrefixMap = new HashMap<>();
        for (String encodedNamespace : allNamespacesEncoded) {
            fixedRegisteredNamespacesEncodedList.add(encodedNamespace);
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
        for (Map.Entry<String, String> entry : additionalPrefixToUrisMappings.entrySet()) {
            String prefix = entry.getKey();
            String uri = entry.getValue();
            String encodedUri = Namespaces.encodeUri(uri);
            if (!fixedRegisteredPrefixesList.contains(prefix)) {
                fixedRegisteredPrefixesList.add(prefix);
            }
            if (!fixedRegisteredNamespacesEncodedList.contains(encodedUri)) {
                fixedRegisteredNamespacesEncodedList.add(encodedUri);
            }
            fixedPrefixToNamespaceMap.put(prefix, uri);
            fixedNamespaceToPrefixMap.put(encodedUri, prefix);
        }
        return new NamespaceRegistryModel(fixedRegisteredPrefixesList, fixedRegisteredNamespacesEncodedList,
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

    // Prefixes that are registered, but not mapped to or from a namespace uri.
    // This kind of inconsistency cannot be fixed automatically, because the namespace uri
    // corresponding to the prefix is unknown.
    public Set<String> getDanglingPrefixes() {
        return danglingPrefixes;
    }

    // Namespace uris that are registered, but not mapped to or from a prefix.
    // This kind of inconsistency cannot be fixed automatically, because the prefix
    // corresponding to the namespace uri is unknown.
    public Set<String> getDanglingEncodedNamespaceUris() {
        return danglingNamespacesEncoded;
    }

    // Broken mappings completed with the missing prefix or namespace uri.
    public Map<String, String> getRepairedMappings() {
        Map<String, String> map = new HashMap<>();
        Set<String> repairablePrefixes = SetUtils.difference(SetUtils.difference(allPrefixes, consistentPrefixes), danglingPrefixes);
        Set<String> repairableUrisEncoded = SetUtils.difference(SetUtils.difference(allNamespacesEncoded, consistentNamespacesEncoded), danglingNamespacesEncoded);
        for (Map.Entry<String, String> entry : prefixToNamespaceMap.entrySet()) {
            String prefix = entry.getKey();
            String uri = entry.getValue();
            if (repairablePrefixes.contains(prefix) || repairableUrisEncoded.contains(uri)) {
                map.put(prefix, uri);
            }
        }
        for (Map.Entry<String, String> entry : namespaceToPrefixMap.entrySet()) {
            String prefix = entry.getValue();
            String uri = entry.getKey();
            if (repairablePrefixes.contains(prefix) || repairableUrisEncoded.contains(uri)) {
                map.put(prefix, uri);
            }
        }
        return map;
    }

    private <T> Set<T> findDuplicates(Collection<T> c) {
        HashSet<T> uniques = new HashSet<>();
        return c.stream().filter(t -> !uniques.add(t)).collect(Collectors.toSet());
    }

    public void dump() throws IOException {
        dump(System.out);
    }

    public void dump(OutputStream out) throws IOException {
        dump(new OutputStreamWriter(out, StandardCharsets.UTF_8));
    }
    
    public void dump(Writer out) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(out)) {
            if (consistent) {
                writer.write("This namespace registry model is consistent, containing the following mappings from prefixes to namespace uris:");
                writer.newLine();
                writer.newLine();
                for (Map.Entry<String, String> entry : prefixToNamespaceMap.entrySet()) {
                    writer.write(entry.getKey() + " -> " + entry.getValue());
                    writer.newLine();
                }
            } else {
                writer.write("This namespace registry model is inconsistent. The inconsistency can " + (isFixable()? "" : "NOT ") + "be fixed.");
                writer.newLine();
                writer.newLine();
                writer.write("Registered prefixes without any namespace mapping: " + danglingPrefixes);
                writer.newLine();
                writer.write("Registered (encoded) namespace URIs without any prefix mapping: " + danglingNamespacesEncoded);
                writer.newLine();
                writer.write("Duplicate prefixes: " + duplicatePrefixes);
                writer.newLine();
                writer.write("Duplicate (encoded) namespace URIs: " + duplicateNamespacesEncoded);
                writer.newLine();
                writer.write("Mapped unregistered prefixes: " + SetUtils.difference(SetUtils.union(mappedPrefixes, mappedToPrefixes), registeredPrefixes));
                writer.newLine();
                writer.write("Mapped unregistered (encoded) namespace URIs: " + SetUtils.difference(SetUtils.union(mappedNamespaces, mappedToNamespacesEncoded), registeredNamespacesEncoded));
                writer.newLine();
                writer.write("Mapped prefixes without a reverse mapping: " + SetUtils.difference(mappedToPrefixes, mappedPrefixes));
                writer.newLine();
                writer.write("Mapped (encoded) namespace URIs without a reverse mapping: " + SetUtils.difference(mappedToNamespacesEncoded, mappedNamespaces));
                writer.newLine();
                writer.newLine();
                if (isFixable()) {
                    NamespaceRegistryModel repaired = tryRegistryRepair();
                    writer.newLine();
                    writer.write("The following mappings could be repaired:");
                    writer.newLine();
                    writer.newLine();
                    for (Map.Entry<String, String> entry : getRepairedMappings().entrySet()) {
                        writer.write(entry.getKey() + " -> " + entry.getValue());
                        writer.newLine();
                    }
                    writer.newLine();
                    writer.newLine();
                    writer.write("The repaired registry would contain the following mappings:");
                    writer.newLine();
                    writer.newLine();
                    for (Map.Entry<String, String> entry : repaired.prefixToNamespaceMap.entrySet()) {
                        writer.write(entry.getKey() + " -> " + entry.getValue());
                        writer.newLine();
                    }
                } else {
                    writer.write("The following mappings could be repaired automatically:");
                    writer.newLine();
                    writer.newLine();
                    for (Map.Entry<String, String> entry : getRepairedMappings().entrySet()) {
                        writer.write(entry.getKey() + " -> " + entry.getValue());
                        writer.newLine();
                    }
                    writer.newLine();
                    writer.newLine();
                    writer.write("To create a fixed model, use #tryRegistryRepair(Map<String, String>) and supply missing prefix to namespace mappings as parameters");
                    writer.newLine();
                }
            }
        }
    }
}
