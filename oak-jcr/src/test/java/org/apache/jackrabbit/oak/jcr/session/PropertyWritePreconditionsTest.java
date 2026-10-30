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

import javax.jcr.AccessDeniedException;
import javax.jcr.Property;
import javax.jcr.Value;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.jcr.version.VersionManagerImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.value.ValueFactoryImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PropertyWritePreconditionsTest {
    private final SessionDelegate session = mock(SessionDelegate.class);
    private final SessionContext context = mock(SessionContext.class);
    private final NodeDelegate parent = mock(NodeDelegate.class);
    private final PropertyDelegate delegate = mock(PropertyDelegate.class);
    private PropertyImpl property;
    private boolean nested;

    @Before
    public void setUp() throws Exception {
        when(context.getSessionDelegate()).thenReturn(session);
        when(context.getValueFactory()).thenReturn(ValueFactoryImpl.getInstance());
        when(context.getOakName("p")).thenReturn("p");
        WorkspaceImpl workspace = mock(WorkspaceImpl.class);
        VersionManagerImpl versions = mock(VersionManagerImpl.class);
        when(context.getWorkspace()).thenReturn(workspace);
        when(workspace.internalGetVersionManager()).thenReturn(versions);
        when(versions.isCheckedOut(any(NodeDelegate.class))).thenReturn(true);
        when(delegate.getParent()).thenReturn(parent);
        when(delegate.getName()).thenReturn("p");
        when(delegate.getPropertyState()).thenReturn(PropertyStates.createProperty("p", "old"));
        when(parent.getPropertyOrNull("p")).thenReturn(delegate);
        doAnswer(i -> {
            SessionOperation<?> operation = i.getArgument(0);
            if (!nested) {
                operation.checkPreconditions();
            }
            operation.performVoid();
            return null;
        }).when(session).performVoid(any());
        when(session.perform(any())).thenAnswer(i -> {
            SessionOperation<?> operation = i.getArgument(0);
            if (!nested) {
                operation.checkPreconditions();
            }
            return operation.perform();
        });
        property = new PropertyImpl(delegate, context);
    }

    /** Checks checkout state without materializing a JCR parent for writes. */
    @Test
    public void writesDoNotReadParentPrimaryType() throws Exception {
        Value value = ValueFactoryImpl.getInstance().createValue("new");
        property.setValue(value);
        when(delegate.getPropertyState()).thenReturn(
                PropertyStates.createProperty("p", Collections.singletonList("old"), Type.STRINGS));
        property.setValue(new Value[] {value});
        property.remove();
        verify(parent, never()).getPropertyOrNull(JcrConstants.JCR_PRIMARYTYPE);
        verify(delegate).setState(PropertyStates.createProperty("p", "new"));
        verify(delegate).setState(PropertyStates.createProperty("p", Collections.singletonList("new"), Type.STRINGS));
        verify(delegate).remove();
    }

    /** Retains access-denied behavior when the property's parent is invisible. */
    @Test(expected = AccessDeniedException.class)
    public void inaccessibleParent() throws Exception {
        when(delegate.getParent()).thenReturn(null);
        property.remove();
    }

    /** Resolves a property only once across removal preconditions and execution. */
    @Test
    public void nodeRemovalResolvesOnce() throws Exception {
        Property result = new NodeImpl<>(parent, context).setProperty("p", (Value) null);
        assertNotNull(result);
        verify(parent).getPropertyOrNull("p");
        verify(delegate).remove();
    }

    /** Still resolves a property when reentrant calls skip preconditions. */
    @Test
    public void nestedRemoval() throws Exception {
        nested = true;
        assertNotNull(new NodeImpl<>(parent, context).setProperty("p", (Value) null));
        verify(parent).getPropertyOrNull("p");
        verify(delegate).remove();
    }

    /** Preserves the non-null removed-property placeholder for an absent property. */
    @Test
    public void absentRemoval() throws Exception {
        when(parent.getPropertyOrNull("p")).thenReturn(null);
        when(parent.getProperty("p")).thenReturn(delegate);
        assertNotNull(new NodeImpl<>(parent, context).setProperty("p", (Value) null));
        verify(parent).getPropertyOrNull("p");
        verify(parent).getProperty("p");
        verify(delegate, never()).remove();
    }
}
