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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class EntryCache implements Constants {

    private final RestrictionProvider restrictionProvider;
    private final PrivilegeBitsProvider bitsProvider;

    /**
     * Mapping effective path (empty string representing the null path) to the permission entries defined for each
     * effective path. Note that this map does not record the name or nature (group vs non-group) of the principal for
     * which the entries have been defined. Similarly it ignores the order of entries as the implementation only
     * supports 'allow' entries.
     */
    private final Map<String, List<PermissionEntry>> entries = new HashMap<>();

    EntryCache(@NotNull Root root, @NotNull Iterable<String> principalPathSet, @NotNull RestrictionProvider restrictionProvider) {
        this.restrictionProvider = restrictionProvider;
        this.bitsProvider = new PrivilegeBitsProvider(root);

        for (String principalPath : principalPathSet) {
            Tree policyTree = root.getTree(PathUtils.concat(principalPath, Constants.REP_PRINCIPAL_POLICY));
            if (!policyTree.exists()) {
                continue;
            }
            for (Tree child : policyTree.getChildren()) {
                if (Constants.NT_REP_PRINCIPAL_ENTRY.equals(TreeUtil.getPrimaryTypeName(child))) {
                    PermissionEntryImpl entry = new PermissionEntryImpl(child);
                    String key = Strings.nullToEmpty(entry.effectivePath);
                    List<PermissionEntry> list = entries.computeIfAbsent(key, k -> new ArrayList<>());
                    list.add(entry);
                }
            }
        }
    }

    @NotNull
    Iterator<PermissionEntry> getEntries(@NotNull String path) {
        Iterable<PermissionEntry> list = entries.get(path);
        return (list == null) ? Collections.emptyIterator() : list.iterator();
    }

    private final class PermissionEntryImpl implements PermissionEntry {

        private final String effectivePath;
        private final PrivilegeBits privilegeBits;
        private RestrictionPattern pattern;

        private PermissionEntryImpl(@NotNull Tree entryTree) {
            effectivePath = Strings.emptyToNull(TreeUtil.getString(entryTree, REP_EFFECTIVE_PATH));
            privilegeBits = bitsProvider.getBits(entryTree.getProperty(REP_PRIVILEGES).getValue(Type.NAMES));
            pattern = restrictionProvider.getPattern(effectivePath, restrictionProvider.readRestrictions(effectivePath, entryTree));
        }

        @NotNull
        public PrivilegeBits getPrivilegeBits() {
            return privilegeBits;
        }

        @Override
        public boolean appliesTo(@NotNull String path) {
            return Text.isDescendantOrEqual(effectivePath, path);
        }

        @Override
        public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
            return pattern.matches(tree, property);
        }

        @Override
        public boolean matches(@NotNull String treePath) {
            return pattern.matches(treePath);
        }

        @Override
        public boolean matches() {
            return pattern.matches();
        }
    }
}