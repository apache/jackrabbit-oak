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

import javax.jcr.AccessDeniedException;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class JackrabbitSessionTest {
    
    @Test
    public void testGetParentOrNull() throws Exception {
        JackrabbitSession s = mock(JackrabbitSession.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
        Node parent = mock(Node.class);
        Item item = mock(Item.class);
        
        when(item.getParent()).thenReturn(parent).getMock();
        assertSame(parent, s.getParentOrNull(item));
        
        doThrow(new AccessDeniedException()).when(item).getParent();
        assertNull(s.getParentOrNull(item));

        doThrow(new ItemNotFoundException()).when(item).getParent();
        assertNull(s.getParentOrNull(item));
    }
}
