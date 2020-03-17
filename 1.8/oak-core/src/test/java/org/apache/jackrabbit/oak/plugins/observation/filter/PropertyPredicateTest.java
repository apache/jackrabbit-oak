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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class PropertyPredicateTest {

    @Test
    public void propertyMatch() {
        String name = "foo";
        final String value = "bar";
        PropertyPredicate p = new PropertyPredicate(name, new Predicate<PropertyState>() {
            @Override
            public boolean apply(PropertyState property) {
                return value.equals(property.getValue(STRING));
            }
        });

        assertTrue(p.apply(createWithProperty(name, value)));
    }

    @Test
    public void propertyMiss() {
        String name = "foo";
        final String value = "bar";
        PropertyPredicate p = new PropertyPredicate(name, new Predicate<PropertyState>() {
            @Override
            public boolean apply(PropertyState property) {
                return "baz".equals(property.getValue(STRING));
            }
        });

        assertFalse(p.apply(createWithProperty(name, value)));
    }

    @Test
    public void nonExistingProperty() {
        String name = "foo";
        final String value = "bar";
        PropertyPredicate p = new PropertyPredicate("any", new Predicate<PropertyState>() {
            @Override
            public boolean apply(PropertyState property) {
                return value.equals(property.getValue(STRING));
            }
        });

        assertFalse(p.apply(createWithProperty(name, value)));
    }

    private static NodeState createWithProperty(String name, String value) {
        return EMPTY_NODE.builder()
                .setProperty(name, value)
                .getNodeState();
    }


}
