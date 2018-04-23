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

package org.apache.jackrabbit.oak.segment.file.proc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class ReferencesNodeTest {

    @Test
    public void shouldExposeReference() {
        List<String> references = Collections.singletonList("u");

        Backend backend = mock(Backend.class);
        when(backend.getSegmentReferences("s")).thenReturn(Optional.of(references));

        assertTrue(new ReferencesNode(backend, "s").hasChildNode("u"));
    }

    @Test
    public void shouldExposeAllReferences() {
        List<String> references = Arrays.asList("u", "v", "w");

        Backend backend = mock(Backend.class);
        when(backend.getSegmentReferences("s")).thenReturn(Optional.of(references));

        assertEquals(Sets.newHashSet(references), Sets.newHashSet(new ReferencesNode(backend, "s").getChildNodeNames()));
    }

}
