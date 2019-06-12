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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import com.google.common.primitives.Ints;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PolicyComparatorTest {

    private final PolicyComparator comparator = new PolicyComparator();

    @Test
    public void testSame() {
        JackrabbitAccessControlPolicy policy = mock(JackrabbitAccessControlPolicy.class);
        assertEquals(0, comparator.compare(policy, policy));
    }

    @Test
    public void testEquals() {
        JackrabbitAccessControlPolicy policy1 = new JackrabbitAccessControlPolicy() {
            @Override
            public String getPath() {
                throw new UnsupportedOperationException();
            }
            @Override
            public boolean equals(Object object) {
                return object instanceof JackrabbitAccessControlPolicy;
            }
        };
        JackrabbitAccessControlPolicy policy2 = () -> {
            throw new UnsupportedOperationException();
        };
        assertEquals(0, comparator.compare(policy1, policy2));
    }

    @Test
    public void testNullPath1() {
        JackrabbitAccessControlPolicy policy1 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn(null).getMock();
        JackrabbitAccessControlPolicy policy2 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/some/path").getMock();
        assertEquals(-1, comparator.compare(policy1, policy2));
    }

    @Test
    public void testNullPath2() {
        JackrabbitAccessControlPolicy policy1 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/some/path").getMock();
        JackrabbitAccessControlPolicy policy2 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn(null).getMock();
        assertEquals(1, comparator.compare(policy1, policy2));
    }

    @Test
    public void testEqualPath() {
        JackrabbitAccessControlPolicy policy1 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/some/path").getMock();
        JackrabbitAccessControlPolicy policy2 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/some/path").getMock();
        assertEquals(0, comparator.compare(policy1, policy2));
    }

    @Test
    public void testEqualDepth() {
        JackrabbitAccessControlPolicy policy1 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/some/path1").getMock();
        JackrabbitAccessControlPolicy policy2 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/some/path2").getMock();
        int expected = "/some/path1".compareTo("/some/path2");
        assertEquals(expected, comparator.compare(policy1, policy2));
    }

    @Test
    public void testPath1Deeper() {
        JackrabbitAccessControlPolicy policy1 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/some/deeper/path").getMock();
        JackrabbitAccessControlPolicy policy2 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/").getMock();
        int expected = Ints.compare(PathUtils.getDepth("/some/deeper/path"), PathUtils.getDepth("/"));
        assertEquals(expected, comparator.compare(policy1, policy2));
    }

    @Test
    public void testPath2Deeper() {
        JackrabbitAccessControlPolicy policy1 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/path").getMock();
        JackrabbitAccessControlPolicy policy2 = when(mock(JackrabbitAccessControlPolicy.class).getPath()).thenReturn("/a/deeper/path").getMock();
        int expected = Ints.compare(PathUtils.getDepth("/path"), PathUtils.getDepth("/a/deeper/path"));
        assertEquals(expected, comparator.compare(policy1, policy2));
    }
}