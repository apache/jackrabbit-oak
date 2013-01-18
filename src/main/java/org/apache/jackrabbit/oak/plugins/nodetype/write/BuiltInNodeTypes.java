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
import java.util.Map;
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
import org.apache.jackrabbit.oak.namepath.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;

import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * {@code BuiltInNodeTypes} is a utility class that registers the built-in
 * node types required for a JCR repository running on Oak.
 */
class BuiltInNodeTypes {

    private final NodeTypeManager ntMgr;

    private final NamespaceRegistry nsReg;

    private final ValueFactory vf;

    private BuiltInNodeTypes(final Root root) {
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

        this.nsReg = new ReadWriteNamespaceRegistry() {
            @Override
            protected Tree getReadTree() {
                return root.getTree("/");
            }
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };

        this.vf = new ValueFactoryImpl(null, new NamePathMapperImpl(
                new GlobalNameMapper() {
                    @Override
                    protected Map<String, String> getNamespaceMap() {
                        return Namespaces.getNamespaceMap(root.getTree("/"));
                    }
                }));
    }

    /**
     * Registers built in node types using the given {@link Root}.
     *
     * @param root the {@link Root} instance.
     */
    public static void register(final Root root) {
        new BuiltInNodeTypes(root).registerBuiltinNodeTypes();
    }

    private void registerBuiltinNodeTypes() {
        // FIXME: migrate custom node types as well.
        try {
            InputStream stream = BuiltInNodeTypes.class.getResourceAsStream("builtin_nodetypes.cnd");
            try {
                CndImporter.registerNodeTypes(
                        new InputStreamReader(stream, Charsets.UTF_8),
                        "built-in node types", ntMgr, nsReg, vf, false);
            } finally {
                stream.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to read built-in node types", e);
        } catch (ParseException e) {
            throw new IllegalStateException(
                    "Unable to parse built-in node types", e);
        } catch (RepositoryException e) {
            throw new IllegalStateException(
                    "Unable to register built-in node types", e);
        }
    }

}
