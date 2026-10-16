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
package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyDefinitionTest {

    private NodeState root = INITIAL_CONTENT;

    @Test
    public void analyzerNameIsParsedFromPropertyDefinition() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .analyzed()
                .getBuilderTree()
                .setProperty(FulltextIndexConstants.PROP_ANALYZER, "frenchText");

        IndexDefinition defn = IndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        IndexDefinition.IndexingRule rule = defn.getApplicableIndexingRule(NT_BASE);
        PropertyDefinition pd = rule.getConfig("foo");

        assertEquals("frenchText", pd.analyzerName);
    }

    @Test
    public void analyzerNameDefaultsToNull() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base").property("foo").analyzed();

        IndexDefinition defn = IndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        IndexDefinition.IndexingRule rule = defn.getApplicableIndexingRule(NT_BASE);
        PropertyDefinition pd = rule.getConfig("foo");

        assertNull(pd.analyzerName);
    }
}
