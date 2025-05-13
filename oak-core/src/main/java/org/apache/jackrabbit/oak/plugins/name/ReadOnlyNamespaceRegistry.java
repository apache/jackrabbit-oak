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

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.collections.StreamUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Read-only namespace registry. Used mostly internally when access to the
 * in-content registered namespaces is needed. See the
 * {@link ReadWriteNamespaceRegistry} subclass for a more complete registry
 * implementation that supports also namespace modifications and that's thus
 * better suited for use in in implementing the full JCR API.
 */
public class ReadOnlyNamespaceRegistry
        implements NamespaceRegistry, NamespaceConstants {

    private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyNamespaceRegistry.class);

    private static volatile boolean CONSISTENCY_CHECKED;

    protected final Tree namespaces;
    protected final Tree nsdata;

    public ReadOnlyNamespaceRegistry(Root root) {
        this.namespaces = root.getTree(NAMESPACES_PATH);
        this.nsdata = namespaces.getChild(REP_NSDATA);
        if (!CONSISTENCY_CHECKED) {
            checkConsistency();
        }
    }

    private Iterable<String> getNSData(String name) {
        PropertyState property = nsdata.getProperty(name);
        if (property != null && property.getType() == STRINGS) {
            return property.getValue(STRINGS);
        } else {
            return emptyList();
        }
    }

    //--------------------------------------------------< NamespaceRegistry >---

    @Override
    public void registerNamespace(String prefix, String uri)
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void unregisterNamespace(String prefix) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override @NotNull
    public String[] getPrefixes() {
        List<String> prefixes = new ArrayList();
        getNSData(REP_PREFIXES).forEach(prefixes::add);
        return prefixes.toArray(new String[prefixes.size()]);
    }

    @Override @NotNull
    public String[] getURIs() {
        List<String> uris = new ArrayList<>();
        getNSData(REP_URIS).forEach(uris::add);
        return uris.toArray(new String[uris.size()]);
    }

    @Override @NotNull
    public String getURI(String prefix) throws NamespaceException {
        if (prefix.isEmpty()) {
            return prefix; // the default empty namespace
        }

        PropertyState property = namespaces.getProperty(prefix);
        if (property != null && property.getType() == STRING) {
            return property.getValue(STRING);
        }

        throw new NamespaceException(
                "No namespace registered for prefix " + prefix);
    }

    @Override @NotNull
    public String getPrefix(String uri) throws NamespaceException {
        if (uri.isEmpty()) {
            return uri; // the default empty namespace
        }

        PropertyState property = nsdata.getProperty(Namespaces.encodeUri(uri));
        if (property != null && property.getType() == STRING) {
            return property.getValue(STRING);
        }

        throw new NamespaceException(
                "No namespace prefix registered for URI " + uri);
    }

    protected void checkConsistency() throws IllegalStateException {
        NamespaceRegistryModel model = NamespaceRegistryModel.create(namespaces);
        if (!model.isConsistent()) {
            LOG.warn("Namespace registry is inconsistent. "
                    + "Unregistered mapped prefixes: {}. "
                    + "Unregistered mapped namespaces: {}. "
                    + "Registered unmapped prefixes: {}. "
                    + "Registered unmapped namespaces: {}.",
                    model.getUnregisteredMappedPrefixes(),
                    model.getUnregisteredMappedNamespaces(),
                    model.getRegisteredUnmappedPrefixes(),
                    model.getRegisteredUnmappedNamespaces());
        }
        CONSISTENCY_CHECKED = true;
    }

    protected static final class NamespaceRegistryModel {
        protected final Set<String> registeredPrefixes;
        protected final Set<String> registeredNamespacesEncoded;
        protected final Map<String, String> prefixToNamespaceMap;
        protected final Map<String, String> namespaceToPrefixMap;

        protected Set<String> mappedPrefixes;
        protected Set<String> mappedNamespaces;
        protected Set<String> mappedToPrefixes;
        protected Set<String> mappedToNamespacesEncoded;
        protected Set<String> allPrefixes;
        protected Set<String> allNamespacesEncoded;
        protected Set<String> consistentPrefixes;
        protected Set<String> consistentNamespaces;
        protected int registrySize;

        private boolean consistent = false;
        private boolean fixable = false;

        private NamespaceRegistryModel(
                Set<String> registeredPrefixes, Set<String> registeredNamespacesEncoded,
                // prefixes to URIs
                Map<String, String> prefixToNamespaceMap,
                // encoded URIs to prefixes
                Map<String, String> namespaceToPrefixMap) {
            // ignore the empty namespace which is not mapped
            this.registeredPrefixes = registeredPrefixes.stream().filter(s -> !Objects.isNull(s) && s.isEmpty()).collect(Collectors.toSet());
            this.registeredNamespacesEncoded = registeredNamespacesEncoded.stream().filter(s -> !Objects.isNull(s) && s.isEmpty()).collect(Collectors.toSet());
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
            consistent = consistentPrefixes.size() == consistentNamespaces.size()
                    && consistentPrefixes.size() == allPrefixes.size();
            if (consistent) {
                fixable = true;
            } else {
                // everything needs to be contained in at least one of the bijective mappings
                fixable = registrySize == SetUtils.union(mappedPrefixes, mappedToPrefixes).size()
                        && registrySize == SetUtils.union(mappedNamespaces, mappedToNamespacesEncoded).size();
            }
        }

        static NamespaceRegistryModel create(Tree namespaces) {
            Tree nsdata = namespaces.getChild(REP_NSDATA);
            Map<String, String> prefixToNamespaceMap = new HashMap<>();
            Map<String, String> namespaceToPrefixMap = new HashMap<>();
            for (PropertyState propertyState : namespaces.getProperties()) {
                String prefix = propertyState.getName();
                if (!prefix.equals(NodeTypeConstants.REP_PRIMARY_TYPE)) {
                    prefixToNamespaceMap.put(prefix, propertyState.getValue(STRING));
                }
            }
            for (PropertyState propertyState : nsdata.getProperties()) {
                String encodedUri = propertyState.getName();
                switch (encodedUri) {
                    case REP_PREFIXES:
                    case REP_URIS:
                    case NodeTypeConstants.REP_PRIMARY_TYPE:
                        break;
                    default:
                        namespaceToPrefixMap.put(encodedUri, propertyState.getValue(STRING));
                }
            }
            NamespaceRegistryModel model = new NamespaceRegistryModel(
                    new HashSet<>(Arrays.asList(IterableUtils.toArray(nsdata.getProperty(REP_PREFIXES).getValue(STRINGS), String.class))),
                    StreamUtils.toStream(nsdata.getProperty(REP_URIS).getValue(STRINGS)).map(Namespaces::encodeUri).collect(Collectors.toSet()),
                    prefixToNamespaceMap, namespaceToPrefixMap);
            return model;
        }

        NamespaceRegistryModel createFixedModel() {
            if (consistent) {
                return this;
            }
            if (!fixable) {
                return null;
            }
            HashSet<String> fixedRegisteredPrefixes = new HashSet<>();
            HashMap<String, String> fixedPrefixToNamespaceMap = new HashMap<>();
            for (String prefix : allPrefixes) {
                if (!mappedPrefixes.contains(prefix)) {
                    for (Map.Entry<String, String> entry : namespaceToPrefixMap.entrySet()) {
                        if (entry.getValue().equals(prefix)) {
                            fixedPrefixToNamespaceMap.put(prefix, Text.unescapeIllegalJcrChars(entry.getKey()));
                            fixedRegisteredPrefixes.add(prefix);
                            break;
                        }
                    }
                }
            }
            HashSet<String> fixedRegisteredNamespacesEncoded = new HashSet<>();
            HashMap<String, String> fixedNamespaceToPrefixMap = new HashMap<>();
            for (String encodedNamespace : allNamespacesEncoded) {
                if (!mappedNamespaces.contains(encodedNamespace)) {
                    for (Map.Entry<String, String> entry : prefixToNamespaceMap.entrySet()) {
                        if (Namespaces.encodeUri(entry.getValue()).equals(encodedNamespace)) {
                            fixedNamespaceToPrefixMap.put(encodedNamespace, entry.getKey());
                            fixedRegisteredNamespacesEncoded.add(encodedNamespace);
                            break;
                        }
                    }
                }
            }
           return new NamespaceRegistryModel(fixedRegisteredPrefixes, fixedRegisteredNamespacesEncoded,
                   fixedPrefixToNamespaceMap, fixedNamespaceToPrefixMap);
        }

        boolean isConsistent() {
            return consistent;
        }

        public boolean isFixable() {
            return fixable;
        }

        Set<String> getUnregisteredMappedPrefixes() {
            return SetUtils.difference(mappedPrefixes, registeredPrefixes);
        }

        Set<String> getRegisteredUnmappedPrefixes() {
            return SetUtils.difference(registeredPrefixes, mappedPrefixes);
        }

        Set<String> getUnregisteredMappedNamespaces() {
            return SetUtils.difference(mappedNamespaces, registeredNamespacesEncoded);
        }

        Set<String> getRegisteredUnmappedNamespaces() {
            return SetUtils.difference(registeredNamespacesEncoded, mappedNamespaces);
        }

        Set<String> getRegisteredPrefixes() {
            return registeredPrefixes;
        }

        Set<String> getRegisteredNamespacesEncoded() {
            return registeredNamespacesEncoded;
        }

        Set<String> getMappedPrefixes() {
            return mappedPrefixes;
        }

        Set<String> getMappedNamespaces() {
            return mappedNamespaces;
        }

        Set<String> getAllPrefixes() {
            return allPrefixes;
        }

        Set<String> getAllNamespaces() {
            return allNamespacesEncoded;
        }
    }
}
