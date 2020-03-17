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
package org.apache.jackrabbit.oak.segment;

import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class CancelableDiffTest {

    @Test
    public void testPropertyAddedInterruptible() throws Throwable {
        PropertyState after = mock(PropertyState.class);

        NodeStateDiff wrapped = mock(NodeStateDiff.class);
        doReturn(true).when(wrapped).propertyAdded(after);

        assertTrue(newCancelableDiff(wrapped, false).propertyAdded(after));
        assertFalse(newCancelableDiff(wrapped, true).propertyAdded(after));
    }

    @Test
    public void testPropertyChangedInterruptible() throws Throwable {
        PropertyState before = mock(PropertyState.class);
        PropertyState after = mock(PropertyState.class);

        NodeStateDiff wrapped = mock(NodeStateDiff.class);
        doReturn(true).when(wrapped).propertyChanged(before, after);

        assertTrue(newCancelableDiff(wrapped, false).propertyChanged(before, after));
        assertFalse(newCancelableDiff(wrapped, true).propertyChanged(before, after));
    }

    @Test
    public void testPropertyDeletedInterruptible() throws Throwable {
        PropertyState before = mock(PropertyState.class);

        NodeStateDiff wrapped = mock(NodeStateDiff.class);
        doReturn(true).when(wrapped).propertyDeleted(before);

        assertTrue(newCancelableDiff(wrapped, false).propertyDeleted(before));
        assertFalse(newCancelableDiff(wrapped, true).propertyDeleted(before));
    }

    @Test
    public void testChildNodeAddedInterruptible() throws Throwable {
        NodeState after = mock(NodeState.class);

        NodeStateDiff wrapped = mock(NodeStateDiff.class);
        doReturn(true).when(wrapped).childNodeAdded("name", after);

        assertTrue(newCancelableDiff(wrapped, false).childNodeAdded("name", after));
        assertFalse(newCancelableDiff(wrapped, true).childNodeAdded("name", after));
    }

    @Test
    public void testChildNodeChangedInterruptible() throws Throwable {
        NodeState before = mock(NodeState.class);
        NodeState after = mock(NodeState.class);

        NodeStateDiff wrapped = mock(NodeStateDiff.class);
        doReturn(true).when(wrapped).childNodeChanged("name", before, after);

        assertTrue(newCancelableDiff(wrapped, false).childNodeChanged("name", before, after));
        assertFalse(newCancelableDiff(wrapped, true).childNodeChanged("name", before, after));
    }

    @Test
    public void testChildNodeDeletedInterruptible() throws Throwable {
        NodeState before = mock(NodeState.class);

        NodeStateDiff wrapped = mock(NodeStateDiff.class);
        doReturn(true).when(wrapped).childNodeDeleted("name", before);

        assertTrue(newCancelableDiff(wrapped, false).childNodeDeleted("name", before));
        assertFalse(newCancelableDiff(wrapped, true).childNodeDeleted("name", before));
    }

    private NodeStateDiff newCancelableDiff(NodeStateDiff wrapped, boolean cancel) {
        return new CancelableDiff(wrapped, Suppliers.ofInstance(cancel));
    }

}
