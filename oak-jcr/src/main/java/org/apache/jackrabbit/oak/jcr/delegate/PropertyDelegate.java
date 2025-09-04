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

import static java.util.Objects.requireNonNull;

import javax.jcr.InvalidItemStateException;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.StringJoiner;

/**
 * {@code PropertyDelegate} serve as internal representations of {@code Property}s.
 * Most methods of this class throw an {@code InvalidItemStateException}
 * exception if the instance is stale. An instance is stale if the underlying
 * items does not exist anymore.
 */
public class PropertyDelegate extends ItemDelegate {

    /** The underlying {@link org.apache.jackrabbit.oak.api.Tree} of this property's parent */
    private final Tree parent;

    @NotNull
    private final String name;

    @Nullable
    private PropertyState state;

    PropertyDelegate(SessionDelegate sessionDelegate, Tree parent, String name) {
        super(sessionDelegate);
        this.parent = requireNonNull(parent);
        this.name = requireNonNull(name);
        this.state = parent.getProperty(name);
    }

    /**
     * The session has been updated since the last time this property delegate
     * was accessed, so we need to re-retrieve the property state to get any
     * potential updates. It might also be that this property was removed,
     * in which case the {@link #state} reference will be {@code null}.
     */
    @Override
    protected void update() {
        state = parent.getProperty(name);
    }

    @Override @NotNull
    public String getName() {
        return name;
    }

    @Override @NotNull
    public String getPath() {
        return PathUtils.concat(parent.getPath(), name);
    }

    @Override @Nullable
    public NodeDelegate getParent() {
        return parent.exists() ? new NodeDelegate(sessionDelegate, parent) : null;
    }

    @Override
    public boolean exists() {
        return state != null;
    }

    @Override @Nullable
    public Status getStatus() {
        return parent.getPropertyStatus(name);
    }

    @Override
    public boolean isProtected() throws InvalidItemStateException {
        NodeDelegate p = getParent();
        if (p != null) {
            return p.isProtected(name, getPropertyState().getType());
        } else {
            throw newInvalidItemStateException();
        }
    }

    @NotNull
    public PropertyState getPropertyState() throws InvalidItemStateException {
        if (state != null) {
            return state;
        } else {
            throw newInvalidItemStateException();
        }
    }

    @NotNull
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

    public String getDate() throws ValueFormatException, InvalidItemStateException {
        return getSingleState().getValue(Type.DATE);
    }

    @NotNull
    public PropertyState getMultiState() throws InvalidItemStateException, ValueFormatException {
        PropertyState p = getPropertyState();
        if (!p.isArray()) {
            throw new ValueFormatException(p + " is single-valued.");
        }
        return p;
    }

    public void setState(@NotNull PropertyState propertyState) {
        parent.setProperty(propertyState);
    }

    /**
     * Remove the property
     */
    @Override
    public boolean remove() {
        if (parent.hasProperty(name)) {
            parent.removeProperty(name);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PropertyDelegate.class.getSimpleName() + "[", "]")
                .add("parent=" + parent)
                .add("property=" + parent.getProperty(name))
                .toString();
    }

    private InvalidItemStateException newInvalidItemStateException() {
        return new InvalidItemStateException(
                "The " + name + " property does not exist");
    }
}
