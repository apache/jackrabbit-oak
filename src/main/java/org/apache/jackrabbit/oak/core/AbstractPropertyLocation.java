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
import org.apache.jackrabbit.oak.commons.PathUtils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AbstractPropertyLocation... TODO
 */
abstract class AbstractPropertyLocation<T extends Tree> extends AbstractTreeLocation {
    protected final AbstractNodeLocation<T> parentLocation;
    protected final String name;

    AbstractPropertyLocation(AbstractNodeLocation<T> parentLocation, String name) {
        this.parentLocation = checkNotNull(parentLocation);
        this.name = checkNotNull(name);
    }

    protected boolean canRead(PropertyState property) {
        return true;
    }

    @Override
    public TreeLocation getParent() {
        return parentLocation;
    }

    @Override
    public boolean exists() {
        Status status = parentLocation.tree.getPropertyStatus(name);
        return status != null && status != Status.DISCONNECTED && getProperty() != null;
    }

    @Override
    public PropertyState getProperty() {
        PropertyState property = parentLocation.getPropertyState(name);
        return canRead(property)
            ? property
            : null;
    }

    @Override
    public Status getStatus() {
        return parentLocation.tree.getPropertyStatus(name);
    }

    @Override
    public String getPath() {
        return PathUtils.concat(parentLocation.getPath(), name);
    }

    @Override
    public boolean remove() {
        parentLocation.tree.removeProperty(name);
        return true;
    }

    @Override
    public boolean set(PropertyState property) {
        parentLocation.tree.setProperty(property);
        return true;
    }
}