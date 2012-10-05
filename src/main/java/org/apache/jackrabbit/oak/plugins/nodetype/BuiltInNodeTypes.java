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
package org.apache.jackrabbit.oak.plugins.nodetype;

import java.io.InputStream;
import java.io.InputStreamReader;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * <code>BuiltInNodeTypes</code> is a utility class that registers the built-in
 * node types required for a JCR repository running on Oak.
 */
public class BuiltInNodeTypes {

    private final ReadWriteNodeTypeManager ntMgr;

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
        if (!nodeTypesInContent()) {
            try {
                InputStream stream = BuiltInNodeTypes.class.getResourceAsStream("builtin_nodetypes.cnd");
                try {
                    ntMgr.registerNodeTypes(new InputStreamReader(stream, "UTF-8"));
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Unable to load built-in node types", e);
            }
        }
    }

    private boolean nodeTypesInContent() {
        Tree types = ntMgr.getTypes();
        return types != null && types.getChildrenCount() > 0;
    }

}
