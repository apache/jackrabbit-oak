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
package org.apache.jackrabbit.oak.plugins.nodetype.write;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeTypeManager;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * {@code BuiltInNodeTypes} is a utility class that registers the built-in
 * node types required for a JCR repository running on Oak.
 */
public final class NodeTypeRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(NodeTypeRegistry.class);

    private static final boolean DEFAULT_REFERENCEABLE_FROZEN_NODE = false;

    private final NodeTypeManager ntMgr;

    private final NamespaceRegistry nsReg;

    private final ValueFactory vf;

    private NodeTypeRegistry(final Root root) {
        this.ntMgr =  new ReadWriteNodeTypeManager() {
            @NotNull
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }

            @NotNull
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };

        this.nsReg = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };

        this.vf = new ValueFactoryImpl(
                root, new NamePathMapperImpl(new GlobalNameMapper(root)));
    }

    /**
     * Register the node type definitions contained in the specified {@code input}
     * using the given {@link Root}.
     *
     * @param root The {@code Root} to register the node types.
     * @param input The input stream containing the node type defintions to be registered.
     * @param systemId An informative id of the given input.
     */
    public static void register(Root root, InputStream input, String systemId) {
        new NodeTypeRegistry(root).registerNodeTypes(input, systemId);
    }

    private void registerNodeTypes(InputStream stream, String systemId) {
        try {
            Reader reader = new InputStreamReader(stream, Charsets.UTF_8);
            // OAK-9134: nt:frozenNode is not implementing mix:referenceable from JCR 2.0.
            // This system property allows to add it back when initializing a repository.
            // PS: To keep supporting tests in fiddling this setting, the SystemPropertySupplier
            // is evaluated here rather than in static code, where this is typically done.
            final boolean referenceableFrozenNode = SystemPropertySupplier.create("oak.referenceableFrozenNode", DEFAULT_REFERENCEABLE_FROZEN_NODE)
                    .loggingTo(LOG).formatSetMessage(
                            (name, value) -> String.format("oak.referenceableFrozenNode set to: %s (using system property %s)", name, value))
                    .get();
            if (referenceableFrozenNode) {
                BufferedReader bufferedReader = new BufferedReader(reader);
                StringBuilder result = new StringBuilder();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    if (line.trim().equals("[nt:frozenNode]")) {
                        line = "[nt:frozenNode] > mix:referenceable";
                    }
                    result.append(line).append(System.lineSeparator());
                }
                reader = new StringReader(result.toString());
            }

            CndImporter.registerNodeTypes(reader, systemId, ntMgr, nsReg, vf, false);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read " + systemId, e);
        } catch (ParseException e) {
            throw new IllegalStateException("Unable to parse " + systemId, e);
        } catch (RepositoryException e) {
            throw new IllegalStateException("Unable to register " + systemId, e);
        }
    }

}
