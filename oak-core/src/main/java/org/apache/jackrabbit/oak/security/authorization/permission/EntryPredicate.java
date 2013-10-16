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
package org.apache.jackrabbit.oak.security.authorization.permission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntryPredicate... TODO
 */
final class EntryPredicate implements Predicate<PermissionEntry> {

    private final Tree tree;
    private final PropertyState property;
    private final String path;

    public EntryPredicate(@Nonnull Tree tree, @Nullable PropertyState property) {
        this.tree = tree;
        this.property = property;
        this.path = tree.getPath();
    }

    public EntryPredicate(@Nonnull String path) {
        this.tree = null;
        this.property = null;
        this.path = path;
    }

    public EntryPredicate() {
        this.tree = null;
        this.property = null;
        this.path = null;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean apply(@Nullable PermissionEntry entry) {
        if (entry == null) {
            return false;
        }
        if (tree != null) {
            return entry.matches(tree, property);
        } else if (path != null) {
            return entry.matches(path);
        } else {
            return entry.matches();
        }
    }
}