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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.SemanticMode;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.slf4j.event.Level;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.SEMANTIC;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticIndexDefinitionTest {

    @Test
    public void semanticModeDefaultsWhenAbsent() {
        assertEquals(SemanticMode.DEFAULT, semanticMode(defn -> {}, 0));
    }

    @Test
    public void semanticModeValues() {
        assertEquals(SemanticMode.DEFAULT, semanticMode(defn -> defn.setProperty(SEMANTIC, "default"), 0));
        assertEquals(SemanticMode.ALWAYS, semanticMode(defn -> defn.setProperty(SEMANTIC, "always"), 0));
        assertEquals(SemanticMode.OFF, semanticMode(defn -> defn.setProperty(SEMANTIC, "off"), 0));
    }

    @Test
    public void semanticModeIsTrimmedAndCaseInsensitive() {
        assertEquals(SemanticMode.ALWAYS, semanticMode(defn -> defn.setProperty(SEMANTIC, " ALWAYS "), 0));
        assertEquals(SemanticMode.OFF, semanticMode(defn -> defn.setProperty(SEMANTIC, "Off"), 0));
    }

    @Test
    public void invalidSemanticModeFallsBackToDefault() {
        assertEquals(SemanticMode.DEFAULT, semanticMode(defn -> defn.setProperty(SEMANTIC, "sometimes"), 1));
        assertEquals(SemanticMode.DEFAULT, semanticMode(defn -> defn.setProperty(SEMANTIC, true), 1));
        assertEquals(SemanticMode.DEFAULT, semanticMode(defn -> defn.setProperty(SEMANTIC, List.of("off"), Type.STRINGS), 1));
        assertEquals(SemanticMode.DEFAULT, semanticMode(defn -> defn.setProperty(binaryProperty(SEMANTIC, "off".getBytes(StandardCharsets.UTF_8))), 1));
    }

    private static SemanticMode semanticMode(Consumer<NodeBuilder> configurer, int expectedWarnings) {
        NodeBuilder defn = new ElasticIndexDefinitionBuilder().build().builder();
        configurer.accept(defn);
        NodeState state = defn.getNodeState();
        LogCustomizer logs = LogCustomizer.forLogger(ElasticIndexDefinition.class.getName())
                .enable(Level.WARN)
                .contains(SEMANTIC)
                .create();
        logs.starting();
        try {
            SemanticMode mode = new ElasticIndexDefinition(state, state, "/oak:index/test", "prefix").semanticMode;
            assertEquals(logs.getLogs().toString(), expectedWarnings, logs.getLogs().size());
            if (expectedWarnings > 0) {
                assertTrue(logs.getLogs().get(0).contains("/oak:index/test"));
            }
            return mode;
        } finally {
            logs.finished();
        }
    }
}
