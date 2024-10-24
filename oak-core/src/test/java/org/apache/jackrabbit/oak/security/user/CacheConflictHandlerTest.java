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

package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler.Resolution;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants.REP_EXPIRATION;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CacheConflictHandlerTest extends AbstractSecurityTest {

    @Test
    public void testChangeChangedPropertyTheirs() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        when(ours.getName()).thenReturn(REP_EXPIRATION);
        when(base.getName()).thenReturn(REP_EXPIRATION);
        when(theirs.getName()).thenReturn(REP_EXPIRATION);

        when(ours.getValue(Type.LONG)).thenReturn(1000L);
        when(base.getValue(Type.LONG)).thenReturn(500L);
        when(theirs.getValue(Type.LONG)).thenReturn(2000L);

        CacheConflictHandler handler = new CacheConflictHandler();
        assertEquals(CacheConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, base));
        PropertyBuilder<Long> merged = PropertyBuilder.scalar(Type.LONG);
        merged.setName(CacheConstants.REP_EXPIRATION);
        merged.setValue(2000L);
        verify(parent).setProperty(merged.getPropertyState());

    }

    @Test
    public void testChangeChangedPropertyBaseBigger() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        when(ours.getName()).thenReturn(REP_EXPIRATION);
        when(base.getName()).thenReturn(REP_EXPIRATION);
        when(theirs.getName()).thenReturn(REP_EXPIRATION);

        when(ours.getValue(Type.LONG)).thenReturn(1000L);
        when(base.getValue(Type.LONG)).thenReturn(2500L);
        when(theirs.getValue(Type.LONG)).thenReturn(1500L);

        CacheConflictHandler handler = new CacheConflictHandler();
        assertEquals(CacheConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, base));

    }

    @Test
    public void testChangeChangedPropertyBaseTheirs() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        when(ours.getName()).thenReturn(REP_EXPIRATION);
        when(base.getName()).thenReturn(REP_EXPIRATION);
        when(theirs.getName()).thenReturn(REP_EXPIRATION);

        when(ours.getValue(Type.LONG)).thenReturn(1000L);
        when(base.getValue(Type.LONG)).thenReturn(1500L);
        when(theirs.getValue(Type.LONG)).thenReturn(1700L);

        CacheConflictHandler handler = new CacheConflictHandler();
        assertEquals(CacheConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, base));

    }

    @Test
    public void testChangeChangedPropertyOur() {

        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        when(ours.getName()).thenReturn(REP_EXPIRATION);
        when(base.getName()).thenReturn(REP_EXPIRATION);
        when(theirs.getName()).thenReturn(REP_EXPIRATION);

        when(ours.getValue(Type.LONG)).thenReturn(2000L);
        when(base.getValue(Type.LONG)).thenReturn(500L);
        when(theirs.getValue(Type.LONG)).thenReturn(1000L);

        CacheConflictHandler handler = new CacheConflictHandler();
        assertEquals(CacheConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, base));
        PropertyBuilder<Long> merged = PropertyBuilder.scalar(Type.LONG);
        merged.setName(CacheConstants.REP_EXPIRATION);
        merged.setValue(2000L);
        verify(parent).setProperty(merged.getPropertyState());

    }

    @Test
    public void testChangeChangedPropertyBase() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        when(ours.getName()).thenReturn(REP_EXPIRATION);
        when(base.getName()).thenReturn(REP_EXPIRATION);
        when(theirs.getName()).thenReturn(REP_EXPIRATION);

        when(ours.getValue(Type.LONG)).thenReturn(1000L);
        when(base.getValue(Type.LONG)).thenReturn(2000L);
        when(theirs.getValue(Type.LONG)).thenReturn(900L);

        CacheConflictHandler handler = new CacheConflictHandler();
        assertEquals(CacheConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, base));

    }

    @Test
    public void testChangeChangedPropertyIgnore() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        CacheConflictHandler handler = new CacheConflictHandler();
        assertEquals(CacheConflictHandler.Resolution.IGNORED, handler.changeChangedProperty(parent, ours, theirs, base));

    }

    @Test
    public void testDifferentTheirsProperty() {
        NodeBuilder parent = mock(NodeBuilder.class);

        PropertyState ours = mock(PropertyState.class);
        PropertyState base = mock(PropertyState.class);
        PropertyState theirs = mock(PropertyState.class);

        when(ours.getName()).thenReturn(REP_EXPIRATION);
        when(base.getName()).thenReturn(REP_EXPIRATION);
        when(theirs.getName()).thenReturn("DifferentProperty");

        CacheConflictHandler handler = new CacheConflictHandler();
        assertEquals(CacheConflictHandler.Resolution.IGNORED, handler.changeChangedProperty(parent, ours, theirs, base));
    }
}