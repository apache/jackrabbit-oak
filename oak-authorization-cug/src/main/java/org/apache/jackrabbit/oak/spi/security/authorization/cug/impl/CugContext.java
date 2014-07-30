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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.util.Text;

/**
 * CugContext... TODO
 */
final class CugContext implements Context, CugConstants {

    private CugContext(){}

    static final Context INSTANCE = new CugContext();

    @Override
    public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
        return CugUtil.definesCug(parent, property);
    }

    @Override
    public boolean definesContextRoot(@Nonnull Tree tree) {
        return CugUtil.definesCug(tree);
    }

    @Override
    public boolean definesTree(@Nonnull Tree tree) {
        return CugUtil.definesCug(tree);
    }

    @Override
    public boolean definesLocation(@Nonnull TreeLocation location) {
        Tree tree = location.getTree();
        if (tree != null && location.exists()) {
            PropertyState p = location.getProperty();
            return (p == null) ? definesTree(tree) : definesProperty(tree, p);
        } else {
            String name = Text.getName(location.getPath());
            return REP_PRINCIPAL_NAMES.equals(name) || REP_CUG_POLICY.equals(name);
        }
    }
}