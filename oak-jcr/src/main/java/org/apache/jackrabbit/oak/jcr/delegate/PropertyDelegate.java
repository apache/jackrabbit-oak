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

import javax.annotation.CheckForNull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

/**
 * {@code PropertyDelegate} serve as internal representations of {@code Property}s.
 * Most methods of this class throw an {@code InvalidItemStateException}
 * exception if the instance is stale. An instance is stale if the underlying
 * items does not exist anymore.
 */
public class PropertyDelegate extends ItemDelegate {

    public PropertyDelegate(SessionDelegate sessionDelegate, TreeLocation location) {
        super(sessionDelegate, location);
    }

    /**
     * Set the value of the property
     *
     * @param value
     */
    public void setValue(Value value) throws RepositoryException {
        if (!getLocation().set(PropertyStates.createProperty(getName(), value))) {
            throw new InvalidItemStateException();
        }
    }

    /**
     * Set the values of the property
     *
     * @param values
     */
    public void setValues(Iterable<Value> values) throws RepositoryException {
        if (!getLocation().set(PropertyStates.createProperty(getName(), values))) {
            throw new InvalidItemStateException();
        }
    }

    /**
     * Remove the property
     */
    public void remove() throws InvalidItemStateException {
        getLocation().remove();
    }

    @CheckForNull
    public PropertyState getPropertyState() throws InvalidItemStateException {
        return getLocation().getProperty();
    }

}
