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
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.core.TreeImpl.PropertyLocation;
import org.apache.jackrabbit.oak.util.TODO;

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
     * @throws IllegalStateException  if {@code isMultivalue()} is {@code true}.
     *
     */
    @Nonnull
    public CoreValue getValue() throws InvalidItemStateException {
        return getPropertyState().getValue();
    }

    /**
     * Get the value of the property
     * @return  the values of the property
     * @throws IllegalStateException  if {@code isMultivalue()} is {@code false}.
     */
    @Nonnull
    public Iterable<CoreValue> getValues() throws InvalidItemStateException {
        return getPropertyState().getValues();
    }

    /**
     * Determine whether the property is multi valued
     * @return  {@code true} if multi valued
     */
    public boolean isMultivalue() throws InvalidItemStateException {
        return getPropertyState().isArray();
    }

    /**
     * Get the property definition of the property
     * @return
     */
    @Nonnull
    public PropertyDefinition getDefinition() {
        try {
            return TODO.dummyImplementation().returnValue(
                new PropertyDefinition() {

                    @Override
                    public int getRequiredType() {
                        return 0;
                    }

                    @Override
                    public String[] getValueConstraints() {
                        // TODO
                        return new String[0];
                    }

                    @Override
                    public Value[] getDefaultValues() {
                        // TODO
                        return new Value[0];
                    }

                    @Override
                    public boolean isMultiple() {
                        // TODO
                        try {
                            return getPropertyState().isArray();
                        }
                        catch (InvalidItemStateException e) {
                            return false;  // todo implement catch e
                        }
                    }

                    @Override
                    public String[] getAvailableQueryOperators() {
                        // TODO
                        return new String[0];
                    }

                    @Override
                    public boolean isFullTextSearchable() {
                        // TODO
                        return false;
                    }

                    @Override
                    public boolean isQueryOrderable() {
                        // TODO
                        return false;
                    }

                    @Override
                    public NodeType getDeclaringNodeType() {
                        // TODO
                        return null;
                    }

                    @Override
                    public String getName() {
                        // TODO
                        try {
                            return getPropertyState().getName();
                        }
                        catch (InvalidItemStateException e) {
                            return null;  // todo implement catch e
                        }
                    }

                    @Override
                    public boolean isAutoCreated() {
                        // TODO
                        return false;
                    }

                    @Override
                    public boolean isMandatory() {
                        // TODO
                        return false;
                    }

                    @Override
                    public int getOnParentVersion() {
                        // TODO
                        return 0;
                    }

                    @Override
                    public boolean isProtected() {
                        // TODO
                        return false;
                    }
                });
        }
        catch (UnsupportedRepositoryOperationException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    /**
     * Set the value of the property
     * @param value
     */
    public void setValue(CoreValue value) throws InvalidItemStateException {
        getLocation().setValue(value);
    }

    /**
     * Set the values of the property
     * @param values
     */
    public void setValues(List<CoreValue> values) throws InvalidItemStateException {
        getLocation().setValues(values);
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
        return getLocation().getProperty();  // Not null
    }

    @Override
    public PropertyLocation getLocation() throws InvalidItemStateException {
        TreeLocation location = super.getLocation();
        if (location.getProperty() == null) {
            throw new InvalidItemStateException("Property is stale");
        }
        return (PropertyLocation) location;
    }

}
