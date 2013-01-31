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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AbstractPropertyLocation... TODO
 */
abstract class AbstractPropertyLocation<T extends Tree, L extends AbstractNodeLocation<T>> implements TreeLocation {

    protected final L parentLocation;
    protected final String name;

    AbstractPropertyLocation(L parentLocation, String name) {
        this.parentLocation = checkNotNull(parentLocation);
        this.name = checkNotNull(name);
    }

    @Override
    public L getParent() {
        return parentLocation;
    }

    @Override
    public TreeLocation getChild(String relPath) {
        return TreeLocation.NULL;
    }

    @Override
    public String getPath() {
        return PathUtils.concat(parentLocation.getPath(), name);
    }

    @Override
    public Tree getTree() {
        return null;
    }
}