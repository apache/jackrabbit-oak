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

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;

/**
 * {@code PropertyDelegate} serve as internal representations of {@code Property}s.
 * Most methods of this class throw an {@code InvalidItemStateException}
 * exception if the instance is stale. An instance is stale if the underlying
 * items does not exist anymore.
 */
public class PropertyDelegate extends ItemDelegate {

    PropertyDelegate(SessionDelegate sessionDelegate, TreeLocation location) {
        super(sessionDelegate, location);
    }

    @Override
    public boolean isProtected() throws InvalidItemStateException {
        return getParent().isProtected(getName());
    }

    @Nonnull
    public PropertyState getPropertyState() throws InvalidItemStateException {
        PropertyState p = getLocation().getProperty();
        if (p == null) {
            throw new InvalidItemStateException();
        }
        return p;
    }

    public boolean isArray() throws InvalidItemStateException {
        return getPropertyState().isArray();
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

    public void setState(PropertyState propertyState) throws InvalidItemStateException {
        if (!getLocation().set(propertyState)) {
            throw new InvalidItemStateException();
        }
    }

    /**
     * Remove the property
     */
    public void remove() throws InvalidItemStateException {
        getLocation().remove();
    }

}
