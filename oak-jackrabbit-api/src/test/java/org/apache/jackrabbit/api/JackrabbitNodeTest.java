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
package org.apache.jackrabbit.api;

import org.junit.Test;
import org.mockito.Answers;

import javax.jcr.Property;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class JackrabbitNodeTest {

    @Test
    public void testGetNodeOrNull() throws Exception {
        JackrabbitNode n = mock(JackrabbitNode.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
        JackrabbitNode child = mock(JackrabbitNode.class);
        
        when(n.hasNode("child")).thenReturn(true);
        when(n.getNode("child")).thenReturn(child);
        
        assertSame(child, n.getNodeOrNull("child"));
        assertNull(n.getNodeOrNull("not/existing"));
        
        verify(n, times(2)).hasNode(anyString());
        verify(n, times(1)).getNode(anyString());
        verify(n, times(2)).getNodeOrNull(anyString());
        verifyNoMoreInteractions(n);
    }

    @Test
    public void testGetPropertyOrNull() throws Exception {
        JackrabbitNode n = mock(JackrabbitNode.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
        Property prop = mock(Property.class);

        when(n.hasProperty("prop")).thenReturn(true);
        when(n.getProperty("prop")).thenReturn(prop);

        assertSame(prop, n.getPropertyOrNull("prop"));
        assertNull(n.getPropertyOrNull("not/existing"));

        verify(n, times(2)).hasProperty(anyString());
        verify(n, times(1)).getProperty(anyString());
        verify(n, times(2)).getPropertyOrNull(anyString());
        verifyNoMoreInteractions(n);
    }
}