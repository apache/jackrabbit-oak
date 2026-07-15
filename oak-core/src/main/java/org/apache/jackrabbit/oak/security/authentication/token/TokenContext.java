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
package org.apache.jackrabbit.oak.security.authentication.token;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants.TOKEN_NT_NAME;

final class TokenContext implements Context {

    private static final Context INSTANCE = new TokenContext();
    
    private TokenContext() {}

    static Context getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean definesProperty(@NotNull Tree parent, @NotNull PropertyState property) {
        return isTokenNode(parent);
    }

    @Override
    public boolean definesContextRoot(@NotNull Tree tree) {
        return TokenConstants.TOKENS_NODE_NAME.equals(tree.getName());
    }

    @Override
    public boolean definesTree(@NotNull Tree tree) {
        return definesContextRoot(tree) || isTokenNode(tree);
    }

    @Override
    public boolean definesLocation(@NotNull TreeLocation location) {
        PropertyState ps = location.getProperty();
        TreeLocation l = (ps != null) ? location.getParent() : location;
        Tree t = l.getTree();
        if (t == null) {
            return false;
        } else {
            return (ps == null) ? definesTree(t) : definesProperty(t, ps);
        }
    }

    @Override
    public boolean definesInternal(@NotNull Tree tree) {
        return false;
    }
    
    private static boolean isTokenNode(@NotNull Tree tree) {
        return TOKEN_NT_NAME.equals(TreeUtil.getPrimaryTypeName(tree));
    }
}