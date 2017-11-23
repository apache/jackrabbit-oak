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

package org.apache.jackrabbit.oak.plugins.index.lucene.binary;

import org.apache.jackrabbit.oak.plugins.index.lucene.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertTrue;

public class BinaryTextExtractorTest {
    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();
    private ExtractedTextCache cache = new ExtractedTextCache(1000, 10000);

    @Test
    public void tikaConfigServiceLoader() throws Exception {
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        BinaryTextExtractor extractor = new BinaryTextExtractor(cache, idxDefn, false);
        assertTrue(extractor.getTikaConfig().getServiceLoader().isDynamic());
    }

}