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
package org.apache.jackrabbit.oak.core;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * DefaultTreeTypeProvider... TODO
 */
public final class TreeTypeProviderImpl implements TreeTypeProvider {

    private final Context contextInfo;

    public TreeTypeProviderImpl(@Nonnull Context contextInfo) {
        this.contextInfo = contextInfo;
    }

    @Override
    public int getType(ImmutableTree tree) {
        if (tree.isRoot()) {
            return TYPE_DEFAULT;
        }

        ImmutableTree parent = tree.getParent();
        int type;
        switch (parent.getType()) {
            case TYPE_HIDDEN:
                type = TYPE_HIDDEN;
                break;
            case TYPE_NODE_TYPE:
                type = TYPE_NODE_TYPE;
                break;
            case TYPE_VERSION:
                type = TYPE_VERSION;
                break;
            case TYPE_AC:
                type = TYPE_AC;
                break;
            default:
                String name = tree.getName();
                if (NodeStateUtils.isHidden(name)) {
                    type = TYPE_HIDDEN;
                } else if (NodeTypeConstants.JCR_NODE_TYPES.equals(name)) {
                    type = TYPE_NODE_TYPE;
                } else if (VersionConstants.VERSION_NODE_NAMES.contains(name) ||
                        VersionConstants.VERSION_NODE_TYPE_NAMES.contains(NodeStateUtils.getPrimaryTypeName(tree.state))) {
                    type = TYPE_VERSION;
                } else if (contextInfo.definesTree(tree)) {
                    type = TYPE_AC;
                } else {
                    type = TYPE_DEFAULT;
                }
        }
        return type;
    }
}
