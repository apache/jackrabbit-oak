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
package org.apache.jackrabbit.oak.jcr;

import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.core.TreeImpl.PropertyLocation;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;

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

    /**
     * Get the value of the property
     * @return  the value of the property
     * @throws InvalidItemStateException
     */
    @Nonnull
    public Value getValue() throws InvalidItemStateException {
        return ValueFactoryImpl.createValue(getPropertyState(), sessionDelegate.getNamePathMapper());
    }

    /**
     * Get the values of the property
     * @return  the values of the property
     * @throws InvalidItemStateException
     */
    @Nonnull
    public List<Value> getValues() throws InvalidItemStateException {
        return ValueFactoryImpl.createValues(getPropertyState(), sessionDelegate.getNamePathMapper());
    }

    /**
     * Determine whether the property is multi valued
     * @return  {@code true} if multi valued
     */
    public boolean isMultivalue() throws InvalidItemStateException {
        return getPropertyState().isArray();
    }

    /**
     * Set the value of the property
     * @param value
     */
    public void setValue(Value value) throws RepositoryException {
        getPropertyLocation().set(PropertyStates.createProperty(getName(), value));
    }

    /**
     * Set the values of the property
     * @param values
     */
    public void setValues(Iterable<Value> values) throws RepositoryException {
        getPropertyLocation().set(PropertyStates.createProperty(getName(), values));
    }

    /**
     * Remove the property
     */
    public void remove() throws InvalidItemStateException {
        getLocation().remove();
    }

    //------------------------------------------------------------< private >---

    @Nonnull
    private PropertyState getPropertyState() throws InvalidItemStateException {
        PropertyState property = getPropertyLocation().getProperty();
        if (property == null) {
            throw new InvalidItemStateException();
        }
        return property;
    }

    @Nonnull
    private PropertyLocation getPropertyLocation() throws InvalidItemStateException {
        TreeLocation location = getLocation();
        if (!(location instanceof PropertyLocation)) {
            throw new InvalidItemStateException();
        }
        return (PropertyLocation) location;
    }

}
