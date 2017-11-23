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

import static com.google.common.collect.Iterables.toArray;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import javax.annotation.Nonnull;
import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;

/**
 * Read-only namespace registry. Used mostly internally when access to the
 * in-content registered namespaces is needed. See the
 * {@link ReadWriteNamespaceRegistry} subclass for a more complete registry
 * implementation that supports also namespace modifications and that's thus
 * better suited for use in in implementing the full JCR API.
 */
public class ReadOnlyNamespaceRegistry
        implements NamespaceRegistry, NamespaceConstants {

    protected final Tree namespaces;
    protected final Tree nsdata;

    public ReadOnlyNamespaceRegistry(Root root) {
        this.namespaces = root.getTree(NAMESPACES_PATH);
        this.nsdata = namespaces.getChild(REP_NSDATA);
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

    @Override @Nonnull
    public String[] getPrefixes() {
        return toArray(getNSData(REP_PREFIXES), String.class);
    }

    @Override @Nonnull
    public String[] getURIs() {
        return toArray(getNSData(REP_URIS), String.class);
    }

    @Override @Nonnull
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

    @Override @Nonnull
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

}
