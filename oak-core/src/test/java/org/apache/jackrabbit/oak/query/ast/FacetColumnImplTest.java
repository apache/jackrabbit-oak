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
package org.apache.jackrabbit.oak.query.ast;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class FacetColumnImplTest {

    @Test
    public void copyOfReturnsFacetColumnImpl() {
        FacetColumnImpl facetColumn = new FacetColumnImpl("a", "rep:facet(jcr:content/metadata/dc:format)", "rep:facet(jcr:content/metadata/dc:format)");
        AstElement copy = facetColumn.copyOf();
        assertTrue("copyOf() must return a FacetColumnImpl, not a plain ColumnImpl", copy instanceof FacetColumnImpl);
    }

    @Test
    public void copyOfReturnsNewInstance() {
        FacetColumnImpl facetColumn = new FacetColumnImpl("a", "rep:facet(prop)", "rep:facet(prop)");
        AstElement copy = facetColumn.copyOf();
        assertTrue(copy instanceof FacetColumnImpl);
        // must be a fresh instance, not the same reference
        assertSame(FacetColumnImpl.class, copy.getClass());
    }
}
