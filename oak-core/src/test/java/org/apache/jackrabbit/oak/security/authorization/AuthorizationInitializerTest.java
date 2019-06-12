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
package org.apache.jackrabbit.oak.security.authorization;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthorizationInitializerTest  {

    private AuthorizationInitializer initializer;
    private NodeBuilder builder;

    @Before
    public void before() {
        Mount m = when(mock(Mount.class).getPathFragmentName()).thenReturn("mount").getMock();
        MountInfoProvider mip = when(mock(MountInfoProvider.class).getNonDefaultMounts()).thenReturn(ImmutableSet.of(m)).getMock();
        initializer = new AuthorizationInitializer(mip);

        builder = mock(NodeBuilder.class);
        when(builder.child(anyString())).thenReturn(builder);
        when(builder.setProperty(anyString(), any(Object.class), any(Type.class))).thenReturn(builder);
        when(builder.setProperty(anyString(), any(Object.class))).thenReturn(builder);
        when(builder.setProperty(any(PropertyState.class))).thenReturn(builder);
    }
    @Test
    public void testFirstInit() {
        when(builder.hasChildNode(anyString())).thenReturn(false);
        when(builder.hasProperty(anyString())).thenReturn(false);

        initializer.initialize(builder, "wspName");

        verify(builder, times(4)).hasChildNode(anyString());
        verify(builder, times(6)).child(anyString());
        verify(builder, times(1)).hasProperty(anyString());
        verify(builder, times(2)).setProperty(any(PropertyState.class));
        verify(builder, times(3)).setProperty(anyString(), any(Object.class));
        verify(builder, times(5)).setProperty(anyString(), any(Object.class), any(Type.class));
    }

    @Test
    public void testSecondInit() {
        when(builder.hasChildNode(anyString())).thenReturn(true);
        when(builder.hasProperty(anyString())).thenReturn(true);

        initializer.initialize(builder, "wspName");

        verify(builder, times(4)).hasChildNode(anyString());
        verify(builder, times(3)).child(anyString());
        verify(builder, times(1)).hasProperty(anyString());
        verify(builder, never()).setProperty(any(PropertyState.class));
        verify(builder, never()).setProperty(anyString(), any(Object.class));
        verify(builder, never()).setProperty(anyString(), any(Object.class), any(Type.class));
    }
}