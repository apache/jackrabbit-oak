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
package org.apache.jackrabbit.oak.jcr.session;

import java.util.Collections;

import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PropertyIterationTest {
    private final Tree tree = mock(Tree.class);
    private final SessionDelegate session = mock(SessionDelegate.class);
    private NodeImpl<NodeDelegate> node;

    @Before
    public void setUp() throws Exception {
        SessionContext context = mock(SessionContext.class);
        when(context.getSessionDelegate()).thenReturn(session);
        when(context.getJcrPath(anyString())).thenAnswer(i -> i.getArgument(0));
        when(session.perform(any())).thenAnswer(i -> {
            SessionOperation<?> operation = i.getArgument(0);
            operation.checkPreconditions();
            return operation.perform();
        });
        when(session.sync(any())).thenAnswer(i -> i.getArgument(0));
        when(tree.exists()).thenReturn(true);
        PropertyState state = PropertyStates.createProperty("title", "value");
        when(tree.getProperties()).thenAnswer(i -> Collections.singletonList(state));
        when(tree.getProperty("title")).thenReturn(state);
        when(tree.getPropertyCount()).thenReturn(1L);
        node = new NodeImpl<>(new NodeDelegate(session, tree), context);
    }

    /** Uses the enumerated state without looking the property up again. */
    @Test
    public void reuseState() throws Exception {
        Property property = node.getProperties().nextProperty();
        assertEquals(PropertyType.STRING, property.getType());
        verify(tree, never()).getProperty("title");
    }

    /** Refreshes an enumerated delegate after a subsequent session update. */
    @Test
    public void updateState() throws Exception {
        Property property = node.getProperties().nextProperty();
        assertEquals(PropertyType.STRING, property.getType());
        when(tree.getProperty("title")).thenReturn(PropertyStates.createProperty("title", 42L, Type.LONG));
        when(session.getUpdateCount()).thenReturn(1L);
        assertEquals(PropertyType.LONG, property.getType());
        verify(tree, times(1)).getProperty("title");
    }

    /** Does not seed a newly consumed delegate with a pre-update iterator snapshot. */
    @Test
    public void updateBeforeConsumption() throws Exception {
        PropertyIterator properties = node.getProperties();
        when(tree.getProperty("title")).thenReturn(PropertyStates.createProperty("title", 42L, Type.LONG));
        when(session.getUpdateCount()).thenReturn(1L);
        assertEquals(PropertyType.LONG, properties.nextProperty().getType());
        verify(tree).getProperty("title");
    }

}
