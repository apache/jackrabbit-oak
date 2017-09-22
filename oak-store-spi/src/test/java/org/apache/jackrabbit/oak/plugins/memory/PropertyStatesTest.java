/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.memory;

import java.util.Calendar;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class PropertyStatesTest {

    private final NamePathMapper namePathMapper = Mockito.mock(NamePathMapper.class);

    @Test
    public void namePropertyFromNameValue() throws RepositoryException {
        PropertyState nameProperty = PropertyStates.createProperty("name", "oak-prefix:value", PropertyType.NAME);
        Value nameValue = ValueFactoryImpl.createValue(nameProperty, namePathMapper);
        PropertyState namePropertyFromValue = PropertyStates.createProperty("name", nameValue);
        assertEquals(nameProperty, namePropertyFromValue);
    }

    @Test
    public void pathPropertyFromPathValue() throws RepositoryException {
        PropertyState pathProperty = PropertyStates.createProperty("path", "oak-prefix:a/oak-prefix:b", PropertyType.PATH);
        Value nameValue = ValueFactoryImpl.createValue(pathProperty, namePathMapper);
        PropertyState namePropertyFromValue = PropertyStates.createProperty("path", nameValue);
        assertEquals(pathProperty, namePropertyFromValue);
    }

    @Test
    public void dateValueFromDateProperty() throws RepositoryException {
        String expected = ISO8601.format(Calendar.getInstance());
        PropertyState dateProperty = PropertyStates.createProperty(
                "date", expected, Type.DATE);
        String actual = dateProperty.getValue(Type.DATE);
        assertEquals(expected, actual);
    }
}
