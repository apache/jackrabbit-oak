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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Root;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NAMESPACES;

/**
 * A model of the namespace registry, containing the mappings from prefixes to
 * namespace URIs and vice versa.
 * <p>
 * The model is created from the namespace registry stored in the repository.
 * It can be used to check the consistency of the registry, repair it if
 * possible, and apply the changes back to the repository.
 */
public final class NamespaceRegistryModel {

    private final Map<String, String> prefixToNamespaceMap;
    private final Map<String, String> encodedNamespaceToPrefixMap;

    private final Set<String> registeredPrefixes;
    private final Set<String> registeredNamespacesEncoded;
    private final Set<String> mappedPrefixes;
    private final Set<String> mappedNamespacesEncoded;
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

    private volatile boolean consistent = false;
    private volatile boolean fixable = false;

    private NamespaceRegistryModel(
            List<String> registeredPrefixesList, List<String> registeredNamespacesEncodedList,
            // prefixes to URIs
            Map<String, String> prefixToNamespaceMap,
            // encoded URIs to prefixes
            Map<String, String> encodedNamespaceToPrefixMap) {
        // ignore the empty namespace which is not mapped
        registeredPrefixes = registeredPrefixesList.stream().filter(s -> !(Objects.isNull(s) || s.isEmpty())).collect(Collectors.toSet());
        duplicatePrefixes = findDuplicates(registeredPrefixesList);
        registeredNamespacesEncoded = registeredNamespacesEncodedList.stream().filter(s -> !(Objects.isNull(s) || s.isEmpty())).collect(Collectors.toSet());
        duplicateNamespacesEncoded = findDuplicates(registeredNamespacesEncodedList);
        this.prefixToNamespaceMap = new HashMap<>(prefixToNamespaceMap);
        this.encodedNamespaceToPrefixMap = new HashMap<>(encodedNamespaceToPrefixMap);
        mappedPrefixes = this.prefixToNamespaceMap.keySet();
        mappedNamespacesEncoded = this.encodedNamespaceToPrefixMap.keySet();
        mappedToPrefixes = new HashSet<>(encodedNamespaceToPrefixMap.values());
        mappedToNamespacesEncoded = this.prefixToNamespaceMap.values().stream().map(Namespaces::encodeUri).collect(Collectors.toSet());
        allPrefixes = SetUtils.union(SetUtils.union(registeredPrefixes, mappedPrefixes), mappedToPrefixes);
        allNamespacesEncoded = SetUtils.union(SetUtils.union(registeredNamespacesEncoded, mappedNamespacesEncoded), mappedToNamespacesEncoded);
        registrySize = Math.max(allPrefixes.size(), allNamespacesEncoded.size());
        consistentPrefixes = SetUtils.intersection(SetUtils.intersection(registeredPrefixes, mappedPrefixes), mappedToPrefixes);
        consistentNamespacesEncoded = SetUtils.intersection(SetUtils.intersection(registeredNamespacesEncoded, mappedNamespacesEncoded), mappedToNamespacesEncoded);
        danglingPrefixes = SetUtils.difference(registeredPrefixes, SetUtils.union(mappedPrefixes, mappedToPrefixes));
        danglingNamespacesEncoded = SetUtils.difference(registeredNamespacesEncoded, SetUtils.union(mappedNamespacesEncoded, mappedToNamespacesEncoded));

        boolean sizeMatches = duplicatePrefixes.isEmpty()
                && duplicateNamespacesEncoded.isEmpty()
                && consistentNamespacesEncoded.size() == allNamespacesEncoded.size()
                && consistentPrefixes.size() == allPrefixes.size();
        boolean doesRoundtrip = true;
        if (sizeMatches) {
            for (String prefix : mappedPrefixes) {
                String revMapped = encodedNamespaceToPrefixMap.get(Namespaces.encodeUri(prefixToNamespaceMap.get(prefix)));
                if (revMapped == null || !revMapped.equals(prefix)) {
                    doesRoundtrip = false;
                    break;
                }
            }
            if (doesRoundtrip) {
                for (String ns : mappedNamespacesEncoded) {
                    String uri = prefixToNamespaceMap.get(encodedNamespaceToPrefixMap.get(ns));
                    if (uri == null || !Namespaces.encodeUri(uri).equals(ns)) {
                        doesRoundtrip = false;
                        break;
                    }
                }
            }
        }
        consistent = sizeMatches && doesRoundtrip;
        fixable = consistent;
        if (!consistent && doesRoundtrip) {
            fixable = registrySize == SetUtils.union(mappedPrefixes, mappedToPrefixes).size()
                    && registrySize == SetUtils.union(mappedNamespacesEncoded, mappedToNamespacesEncoded).size();
        }
    }

    /**
     * Creates a new {@link NamespaceRegistryModel} from the namespace registry
     * stored in the system tree under the given repository {@link Root}.
     *
     * @param root the root of the repository
     * @return a new {@link NamespaceRegistryModel} or {@code null} if the
     *         namespace registry does not exist
     */
    public static @Nullable NamespaceRegistryModel create(@NotNull Root root) {
        Tree rootTree = root.getTree("/");
        Tree namespaces = rootTree.getChild( JcrConstants.JCR_SYSTEM ).getChild(REP_NAMESPACES);
        if (namespaces.exists()) {
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
        } else {
            return null;
        }
    }

    /**
     * Creates a new {@link NamespaceRegistryModel} with the given mappings. Used by {@see NamespaceRegistryCommand} to
     * repair a namespace registry that cannot be fixed automatically because mapping information is missing.
     *
     * @param additionalPrefixToUrisMappings a map from prefixes to namespace URIs
     * @return a new {@link NamespaceRegistryModel}
     */
    public NamespaceRegistryModel setMappings(@NotNull Map<String, String> additionalPrefixToUrisMappings) {
        List<String> newRegisteredPrefixesList = new ArrayList<>(registeredPrefixes);
        HashMap<String, String> newPrefixToNamespaceMap = new HashMap<>(prefixToNamespaceMap);
        List<String> newRegisteredNamespacesEncodedList = new ArrayList<>(registeredNamespacesEncoded);
        HashMap<String, String> newEncodedNamespaceToPrefixMap = new HashMap<>(encodedNamespaceToPrefixMap);
        for (Map.Entry<String, String> entry : additionalPrefixToUrisMappings.entrySet()) {
            String prefix = entry.getKey();
            String uri = entry.getValue();
            String encodedUri = Namespaces.encodeUri(uri);

            if (!newRegisteredPrefixesList.contains(prefix)) {
                newRegisteredPrefixesList.add(prefix);
            }
            if (!newRegisteredNamespacesEncodedList.contains(encodedUri)) {
                newRegisteredNamespacesEncodedList.add(encodedUri);
            }
            String previousUri = newPrefixToNamespaceMap.get(prefix);
            newPrefixToNamespaceMap.put(prefix, uri);
            if (previousUri != null) {
                String previousEncodedUri = Namespaces.encodeUri(previousUri);
                newRegisteredNamespacesEncodedList.remove(previousEncodedUri);
                newEncodedNamespaceToPrefixMap.remove(previousEncodedUri);
            }
            newEncodedNamespaceToPrefixMap.put(encodedUri, prefix);
        }
        return new NamespaceRegistryModel(newRegisteredPrefixesList, newRegisteredNamespacesEncodedList,
                newPrefixToNamespaceMap, newEncodedNamespaceToPrefixMap);
    }

    /** Tries to repair the namespace registry model by fixing the mappings
     * from prefixes to namespace URIs and vice versa. If the model is not
     * fixable, it returns the original model.
     *
     * @return a new {@link NamespaceRegistryModel} with fixed mappings or the
     *         original model if it cannot be fixed
     */
    public NamespaceRegistryModel tryRegistryRepair() {
        if (fixable) {
            List<String> fixedRegisteredPrefixesList = new ArrayList<>();
            HashMap<String, String> fixedPrefixToNamespaceMap = new HashMap<>();
            for (String prefix : allPrefixes) {
                if (mappedPrefixes.contains(prefix)) {
                    fixedRegisteredPrefixesList.add(prefix);
                    fixedPrefixToNamespaceMap.put(prefix, prefixToNamespaceMap.get(prefix));
                } else {
                    for (Map.Entry<String, String> entry : encodedNamespaceToPrefixMap.entrySet()) {
                        if (entry.getValue().equals(prefix)) {
                            fixedRegisteredPrefixesList.add(prefix);
                            fixedPrefixToNamespaceMap.put(prefix, Text.unescape(entry.getKey()));
                            break;
                        }
                    }
                }
            }
            List<String> fixedRegisteredNamespacesEncodedList = new ArrayList<>();
            HashMap<String, String> fixedNamespaceToPrefixMap = new HashMap<>();
            for (String encodedNamespace : allNamespacesEncoded) {
                if (mappedNamespacesEncoded.contains(encodedNamespace)) {
                    fixedRegisteredNamespacesEncodedList.add(encodedNamespace);
                    fixedNamespaceToPrefixMap.put(encodedNamespace, encodedNamespaceToPrefixMap.get(encodedNamespace));
                } else {
                    for (Map.Entry<String, String> entry : prefixToNamespaceMap.entrySet()) {
                        if (Namespaces.encodeUri(entry.getValue()).equals(encodedNamespace)) {
                            fixedRegisteredNamespacesEncodedList.add(encodedNamespace);
                            fixedNamespaceToPrefixMap.put(encodedNamespace, entry.getKey());
                            break;
                        }
                    }
                }
            }
            return new NamespaceRegistryModel(fixedRegisteredPrefixesList, fixedRegisteredNamespacesEncodedList,
                    fixedPrefixToNamespaceMap, fixedNamespaceToPrefixMap);
        }
        return this;
    }

    /**
     * Applies this namespace registry model to the given repository {@link Root}.
     *
     * @param root the root of the repository
     * @throws RepositoryException if an error occurs while applying the changes
     * @throws CommitFailedException if the commit fails
     */
    public void apply(Root root) throws RepositoryException, CommitFailedException {
        Tree rootTree = root.getTree("/");
        Tree namespaces = rootTree.getChild( JcrConstants.JCR_SYSTEM ).getChild(REP_NAMESPACES);
        Tree nsdata = namespaces.getChild(NamespaceConstants.REP_NSDATA);
        for (PropertyState propertyState : namespaces.getProperties()) {
            String name = propertyState.getName();
            if (!JCR_PRIMARYTYPE.equals(name)) {
                namespaces.removeProperty(name);
            }
        }
        for (PropertyState propertyState : nsdata.getProperties()) {
            String name = propertyState.getName();
            if (!JCR_PRIMARYTYPE.equals(name)) {
                nsdata.removeProperty(name);
            }
        }
        nsdata.removeProperty(NamespaceConstants.REP_PREFIXES);
        nsdata.removeProperty(NamespaceConstants.REP_URIS);
        for (Map.Entry<String, String> entry : prefixToNamespaceMap.entrySet()) {
            String prefix = entry.getKey();
            String uri = entry.getValue();
            namespaces.setProperty(prefix, uri);
        }
        for (Map.Entry<String, String> entry : encodedNamespaceToPrefixMap.entrySet()) {
            String encodedUri = entry.getKey();
            String prefix = entry.getValue();
            nsdata.setProperty(encodedUri, prefix);
        }
        nsdata.setProperty(NamespaceConstants.REP_PREFIXES, mappedPrefixes, STRINGS);
        nsdata.setProperty(NamespaceConstants.REP_URIS, prefixToNamespaceMap.values(), STRINGS);
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

    /** Prefixes that are registered, but not mapped to or from a namespace uri.
     * This kind of inconsistency cannot be fixed automatically, because the namespace uri
     * corresponding to the prefix is unknown.
     * Apply the {@link #setMappings(Map)} method to create a new model with the missing mappings.
     */
    public Set<String> getDanglingPrefixes() {
        return danglingPrefixes;
    }

    /** Namespace uris that are registered, but not mapped to or from a prefix.
     * This kind of inconsistency cannot be fixed automatically, because the prefix
     * corresponding to the namespace uri is unknown.
     * Apply the {@link #setMappings(Map)} method to create a new model with the missing mappings.
     */
    public Set<String> getDanglingEncodedNamespaceUris() {
        return danglingNamespacesEncoded;
    }

    /**
     * Broken mappings completed with the missing prefix or namespace uri.
     */
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
        for (Map.Entry<String, String> entry : encodedNamespaceToPrefixMap.entrySet()) {
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

    /**
     * Write a human-readable analysis of the namespace registry model to System.out.
     */
    public void dump() throws IOException {
        dump(System.out);
    }

    /**
     * Write a human-readable analysis of the namespace registry model to the
     * given {@link OutputStream}.
     *
     * @param out the output stream to write to
     * @throws IOException if an error occurs while writing to the output stream
     */
    public void dump(OutputStream out) throws IOException {
        dump(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        out.flush();
    }

    public void dump(Writer out) throws IOException {
        BufferedWriter writer = new BufferedWriter(out);
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
                writer.write("Registered namespace URIs without any prefix mapping: " + danglingNamespacesEncoded);
                writer.newLine();
                writer.write("Duplicate prefixes: " + duplicatePrefixes);
                writer.newLine();
                writer.write("Duplicate namespace URIs: " + duplicateNamespacesEncoded);
                writer.newLine();
                writer.write("Mapped unregistered prefixes: " + SetUtils.difference(SetUtils.union(mappedPrefixes, mappedToPrefixes), registeredPrefixes));
                writer.newLine();
                writer.write("Mapped unregistered namespace URIs: " + SetUtils.difference(SetUtils.union(mappedNamespacesEncoded, mappedToNamespacesEncoded), registeredNamespacesEncoded));
                writer.newLine();
                writer.write("Mapped prefixes without a reverse mapping: " + SetUtils.difference(mappedPrefixes, mappedToPrefixes));
                writer.newLine();
                writer.write("Mapped namespace URIs without a reverse mapping: " + SetUtils.difference(mappedNamespacesEncoded, mappedToNamespacesEncoded));
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
                    writer.write("The following mappings could be repaired:");
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
            writer.flush();
    }
}
