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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;

import java.util.Calendar;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_LAST_SYNCED;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class ExternalIdentityConflictHandlerTest {

    private final ExternalIdentityConflictHandler handler = new ExternalIdentityConflictHandler();

    @Test
    public void testAddExistingProperty() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.addExistingProperty(mock(NodeBuilder.class), mock(PropertyState.class), mock(PropertyState.class)));
    }

    @Test
    public void testAddExistingPropertyRepLastSynced() {
        Calendar cal = Calendar.getInstance();
        String calStr = ISO8601.format(cal);
        cal.set(Calendar.YEAR, 2000);
        String calStr2 = ISO8601.format(cal);

        PropertyState ours = when(mock(PropertyState.class).getName()).thenReturn(REP_LAST_SYNCED).getMock();
        when(ours.getValue(Type.DATE)).thenReturn(calStr);
        PropertyState theirs = when(mock(PropertyState.class).getValue(Type.DATE)).thenReturn(calStr2).getMock();

        assertSame(ThreeWayConflictHandler.Resolution.MERGED, handler.addExistingProperty(mock(NodeBuilder.class), ours, theirs));
    }

    @Test
    public void testChangeChangedProperty() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.changeChangedProperty(mock(NodeBuilder.class), mock(PropertyState.class), mock(PropertyState.class), mock(PropertyState.class)));
    }

    @Test
    public void testChangeChangedPropertyRepLastSynced() {
        Calendar cal = Calendar.getInstance();
        String calStr = ISO8601.format(cal);
        cal.set(Calendar.YEAR, 2000);
        String calStr2 = ISO8601.format(cal);

        PropertyState ours = when(mock(PropertyState.class).getName()).thenReturn(REP_LAST_SYNCED).getMock();
        when(ours.getValue(Type.DATE)).thenReturn(calStr2);
        PropertyState theirs = when(mock(PropertyState.class).getValue(Type.DATE)).thenReturn(calStr).getMock();

        assertSame(ThreeWayConflictHandler.Resolution.MERGED, handler.changeChangedProperty(mock(NodeBuilder.class), ours, theirs, mock(PropertyState.class)));
    }

    @Test
    public void testChangeDeletedProperty() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.changeDeletedProperty(mock(NodeBuilder.class), mock(PropertyState.class), mock(PropertyState.class)));
    }

    @Test
    public void testDeleteDeletedProperty() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteDeletedProperty(mock(NodeBuilder.class), mock(PropertyState.class)));
    }

    @Test
    public void testDeleteChangedProperty() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteChangedProperty(mock(NodeBuilder.class), mock(PropertyState.class), mock(PropertyState.class)));
    }

    @Test
    public void testAddExistingNode() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.addExistingNode(mock(NodeBuilder.class), "name", mock(NodeState.class), mock(NodeState.class)));
    }

    @Test
    public void testChangeDeletedNode() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.changeDeletedNode(mock(NodeBuilder.class), "name", mock(NodeState.class), mock(NodeState.class)));
    }

    @Test
    public void testDeleteChangedNode() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteChangedNode(mock(NodeBuilder.class), "name", mock(NodeState.class), mock(NodeState.class)));
    }

    @Test
    public void testDeleteDeletedNode() {
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteDeletedNode(mock(NodeBuilder.class), "name", mock(NodeState.class)));
    }

    @Test
    public void testMergeOursCannotBeParsed() {
        Calendar cal = Calendar.getInstance();
        String calStr = ISO8601.format(cal);

        PropertyState ours = when(mock(PropertyState.class).getName()).thenReturn(REP_LAST_SYNCED).getMock();
        when(ours.getValue(Type.DATE)).thenReturn("notParseable");
        PropertyState theirs = when(mock(PropertyState.class).getValue(Type.DATE)).thenReturn(calStr).getMock();

        NodeBuilder parent = mock(NodeBuilder.class);
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, mock(PropertyState.class)));
        verify(parent, times(1)).setProperty(REP_LAST_SYNCED, ISO8601.parse(theirs.getValue(Type.DATE)));
    }

    @Test
    public void testMergeTheirsCannotBeParsed() {
        Calendar cal = Calendar.getInstance();
        String calStr = ISO8601.format(cal);

        PropertyState ours = when(mock(PropertyState.class).getName()).thenReturn(REP_LAST_SYNCED).getMock();
        when(ours.getValue(Type.DATE)).thenReturn(calStr);
        PropertyState theirs = when(mock(PropertyState.class).getValue(Type.DATE)).thenReturn("notParseable").getMock();

        NodeBuilder parent = mock(NodeBuilder.class);
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, handler.changeChangedProperty(parent, ours, theirs, mock(PropertyState.class)));
        verify(parent, times(1)).setProperty(REP_LAST_SYNCED, ISO8601.parse(ours.getValue(Type.DATE)));
    }

    @Test
    public void testMergeNoneCannotBeParsed() {
        PropertyState ours = when(mock(PropertyState.class).getName()).thenReturn(REP_LAST_SYNCED).getMock();
        when(ours.getValue(Type.DATE)).thenReturn("notParseable1");
        PropertyState theirs = when(mock(PropertyState.class).getValue(Type.DATE)).thenReturn("notParseable2").getMock();

        NodeBuilder parent = mock(NodeBuilder.class);
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.changeChangedProperty(parent, ours, theirs, mock(PropertyState.class)));
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.addExistingProperty(parent, ours, theirs));
    }
}