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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.facet.FacetsConfig;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for {@link FacetHelper}
 */
public class FacetHelperTest {

    private NodeState root = INITIAL_CONTENT;

    @Test
    public void testGetFacetsConfig() throws Exception {
        FacetsConfig facetsConfig = FacetHelper.getFacetsConfig(root.builder());
        assertNotNull(facetsConfig);
    }

    @Test
    public void testParseFacetField() throws Exception {
        String field = FacetHelper.parseFacetField("rep:facet(text)");
        assertNotNull(field);
        assertEquals("text", field);
        field = FacetHelper.parseFacetField("rep:facet(jcr:title)");
        assertNotNull(field);
        assertEquals("jcr:title", field);
        field = FacetHelper.parseFacetField("rep:facet(jcr:primaryType)");
        assertNotNull(field);
        assertEquals("jcr:primaryType", field);

    }
}