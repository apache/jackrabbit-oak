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
package org.apache.jackrabbit.oak.jcr.delegate;

import static com.google.common.base.Objects.toStringHelper;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * {@code PropertyDelegate} serve as internal representations of {@code Property}s.
 * Most methods of this class throw an {@code InvalidItemStateException}
 * exception if the instance is stale. An instance is stale if the underlying
 * items does not exist anymore.
 */
public class PropertyDelegate extends ItemDelegate {

    /** The underlying {@link org.apache.jackrabbit.oak.api.Tree} of this property's parent */
    private final Tree parent;

    private final String name;

    /**
     * Create a new property delegate for an existing parent.
     * @param sessionDelegate
     * @param parent  parent tree
     * @param name  property name
     * @return  {@code PropertyDelegate} instance or {@code null} if either {@code parent}
     *          does not exist or does not have a property {@code name}.
     */
    @CheckForNull
    static PropertyDelegate create(SessionDelegate sessionDelegate, Tree parent, String name) {
        return parent.exists() && parent.hasProperty(name)
            ? new PropertyDelegate(sessionDelegate, parent, name)
            : null;
    }

    PropertyDelegate(SessionDelegate sessionDelegate, Tree parent, String name) {
        super(sessionDelegate);
        this.parent = parent;
        this.name = name;
    }

    @Override
    @Nonnull
    public String getName() {
        return name;
    }

    @Override
    @Nonnull
    public String getPath() {
        return PathUtils.concat(parent.getPath(), name);
    }

    @Override
    @CheckForNull
    public NodeDelegate getParent() {
        return NodeDelegate.create(sessionDelegate, parent);
    }

    @Override
    public boolean isStale() {
        return !parent.exists() || !parent.hasProperty(name);
    }

    @Override
    @CheckForNull
    public Status getStatus() {
        return parent.getPropertyStatus(name);
    }

    @Override
    public boolean isProtected() throws InvalidItemStateException {
        return getParent().isProtected(name);
    }

    @Nonnull
    public PropertyState getPropertyState() throws InvalidItemStateException {
        PropertyState p = parent.getProperty(name);
        if (p == null) {
            throw new InvalidItemStateException();
        }
        return p;
    }

    @Nonnull
    public PropertyState getSingleState() throws InvalidItemStateException, ValueFormatException {
        PropertyState p = getPropertyState();
        if (p.isArray()) {
            throw new ValueFormatException(p + " is multi-valued.");
        }
        return p;
    }

    public boolean getBoolean() throws ValueFormatException, InvalidItemStateException {
        return getSingleState().getValue(Type.BOOLEAN);
    }

    public String getString() throws ValueFormatException, InvalidItemStateException {
        return getSingleState().getValue(Type.STRING);
    }

    public Long getDate() throws ValueFormatException, InvalidItemStateException {
        return getSingleState().getValue(Type.DATE);
    }

    @Nonnull
    public PropertyState getMultiState() throws InvalidItemStateException, ValueFormatException {
        PropertyState p = getPropertyState();
        if (!p.isArray()) {
            throw new ValueFormatException(p + " is single-valued.");
        }
        return p;
    }

    public void setState(PropertyState propertyState) {
        parent.setProperty(propertyState);
    }

    /**
     * Remove the property
     */
    public void remove() {
        parent.removeProperty(name);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("parent", parent)
                .add("property", parent.getProperty(name))
                .toString();
    }

}
