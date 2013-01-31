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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.api.TreeLocation;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AbstractNodeLocation... TODO
 */
abstract class AbstractNodeLocation<T extends Tree> implements TreeLocation {

    protected final T tree;

    AbstractNodeLocation(T tree) {
        this.tree = checkNotNull(tree);
    }

    @Override
    public boolean exists() {
        Status status = getStatus();
        return status != null && status != Status.REMOVED && getTree() != null;
    }

    @Override
    public String getPath() {
        return tree.getPath();
    }

    @Override
    public PropertyState getProperty() {
        return null;
    }

    @Override
    public Tree.Status getStatus() {
        return tree.getStatus();
    }
}