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
package org.apache.jackrabbit.oak.index;

import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12431">OAK-12431</a>
 */
public class IndexerSupportTest {

    @Test
    public void luceneIndexWithoutIndexRules() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> IndexerSupport.checkIndexRulesExist(indexOfType("lucene"), "/oak:index/fooIndex"));
        String message = e.getMessage();
        assertTrue(message, message.contains("/oak:index/fooIndex"));
        assertTrue(message, message.contains("'indexRules'"));
        assertTrue(message, message.contains("the filter in the index definition is on \"oak:index\", and there is an \"include\" pattern"));
    }

    @Test
    public void elasticIndexWithoutIndexRules() {
        assertThrows(IllegalStateException.class,
                () -> IndexerSupport.checkIndexRulesExist(indexOfType("elasticsearch"), "/oak:index/fooIndex"));
    }

    @Test
    public void fulltextIndexWithIndexRules() {
        NodeBuilder idx = indexOfType("lucene");
        idx.child(FulltextIndexConstants.INDEX_RULES);
        IndexerSupport.checkIndexRulesExist(idx, "/oak:index/fooIndex");
    }

    @Test
    public void nonFulltextIndexWithoutIndexRules() {
        IndexerSupport.checkIndexRulesExist(indexOfType("property"), "/oak:index/fooIndex");
    }

    @Test
    public void indexWithoutType() {
        IndexerSupport.checkIndexRulesExist(EMPTY_NODE.builder(), "/oak:index/fooIndex");
    }

    private static NodeBuilder indexOfType(String type) {
        NodeBuilder idx = EMPTY_NODE.builder();
        idx.setProperty("type", type);
        return idx;
    }
}
