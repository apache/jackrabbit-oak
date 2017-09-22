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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeTypeManager;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * {@code BuiltInNodeTypes} is a utility class that registers the built-in
 * node types required for a JCR repository running on Oak.
 */
public final class NodeTypeRegistry {

    private final NodeTypeManager ntMgr;

    private final NamespaceRegistry nsReg;

    private final ValueFactory vf;

    private NodeTypeRegistry(final Root root) {
        this.ntMgr =  new ReadWriteNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }

            @Nonnull
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
            CndImporter.registerNodeTypes(
                    new InputStreamReader(stream, Charsets.UTF_8),
                    systemId, ntMgr, nsReg, vf, false);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read " + systemId, e);
        } catch (ParseException e) {
            throw new IllegalStateException("Unable to parse " + systemId, e);
        } catch (RepositoryException e) {
            throw new IllegalStateException("Unable to register " + systemId, e);
        }
    }

}
