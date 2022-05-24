/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.IndexWriterConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils.getIndexWriterConfig;
import static org.junit.Assert.assertEquals;

public class LuceneIndexConfigTest {

    private NodeState root;

    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty("oak.index.lucene.maxBufferedDeleteTerms", "1000")
            .and("oak.index.lucene.ramPerThreadHardLimitMB", "100");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties
            = new RestoreSystemProperties();

    @Test
    public void testIndexWriterConfig() {
        root = INITIAL_CONTENT;
        NodeBuilder idx = root.builder();
        LuceneIndexDefinition definition = new LuceneIndexDefinition(root, idx.getNodeState(), "/foo");
        IndexWriterConfig config = getIndexWriterConfig(definition, true);
        assertEquals(config.getRAMBufferSizeMB(), 16.0, .01);
        assertEquals(100, config.getRAMPerThreadHardLimitMB());
        assertEquals(1000, config.getMaxBufferedDeleteTerms());
    }
}
