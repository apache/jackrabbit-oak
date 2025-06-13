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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
            checkConsistency(root);
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

    public boolean checkConsistency(Root root) throws IllegalStateException {
        NamespaceRegistryModel model = createNamespaceRegistryModel(root);
        if (model == null) {
            LOG.warn("Consistency check skipped because there is no namespace registry.");
        }
        return model == null || model.isConsistent();
    }

    public NamespaceRegistryModel createNamespaceRegistryModel(Root root) {
        return NamespaceRegistryModel.create(root);
    }

}
