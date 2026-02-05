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
package org.apache.jackrabbit.oak.plugins.index.optimizer;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.diff.RootIndexesListService;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.mockito.MockedStatic;

public class FindMatchingIndexNameTest {

    @Test
    public void testFindMatchingIndexName() throws IOException {
        String indexJson = "{\n" +
            "  \"index\": {\n" +
            "    \"compatVersion\": 2,\n" +
            "    \"async\": \"async\",\n" +
            "    \"queryPaths\": [\"/content/dam/test\"],\n" +
            "    \"includedPaths\": [\"/content/dam/test\"],\n" +
            "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n" +
            "    \"evaluatePathRestrictions\": true,\n" +
            "    \"type\": \"lucene\",\n" +
            "    \"tags\": [\"fragments\"],\n" +
            "    \"indexRules\": {\n" +
            "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n" +
            "      \"dam:Asset\": {\n" +
            "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n" +
            "        \"properties\": {\n" +
            "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n" +
            "          \"title\": {\n" +
            "            \"name\": \"str:jcr:title\",\n" +
            "            \"propertyIndex\": true,\n" +
            "            \"jcr:primaryType\": \"nam:nt:unstructured\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        try (MockedStatic<?> mockedStatic = mockStatic(RootIndexesListService.class)) {
            NodeStore store = mock(NodeStore.class);

            String indexesJsonString;

            try (InputStream stream = getClass().getResourceAsStream("/org/apache/jackrabbit/oak/plugins/index/diff/indexes.json")) {
                indexesJsonString = IOUtils.toString(stream, StandardCharsets.UTF_8);
            }

            mockedStatic.when(() -> RootIndexesListService.getRootIndexDefinitions(eq(store), anyString()))
                .thenReturn(JsonObject.fromJson(indexesJsonString, true));

            Optional<String> matchingIndexName = DiffIndexUpdater.findMatchingIndexName(store, indexJson);

            assertTrue(matchingIndexName.isPresent());
        }
    }

}
