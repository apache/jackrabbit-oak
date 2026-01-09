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
package org.apache.jackrabbit.oak.plugins.index.diff;

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.junit.Test;

public class MergeTest {

    @Test
    public void renamedProperty() {
        // A property might be indexed twice, by adding two children to the "properties" node
        // that both have the same "name" value.
        // Alternatively, they could have the same "function" value.
        String merged = DiffIndexMerger.processMerge(JsonObject.fromJson("{\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"acme:Test\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"abc\": {\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"name\": \"test\",\n"
                + "            \"boost\": 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "", true), JsonObject.fromJson("{\n"
                        + "    \"indexRules\": {\n"
                        + "      \"acme:Test\": {\n"
                        + "        \"properties\": {\n"
                        + "          \"def\": {\n"
                        + "            \"name\": \"test\",\n"
                        + "            \"boost\": 1.2\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }", true)).toString();
        assertEquals("{\n"
                + "  \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"indexRules\": {\n"
                + "    \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "    \"acme:Test\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"properties\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"abc\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"name\": \"test\",\n"
                + "          \"boost\": 1.2\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }

    @Test
    public void renamedFunction() {
        // A function might be indexed twice, by adding two children to the "properties" node
        // that both have the same "function" value.
        String merged = DiffIndexMerger.processMerge(JsonObject.fromJson("{\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"acme:Test\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"abc\": {\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"function\": \"upper(test)\",\n"
                + "            \"boost\": 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "", true), JsonObject.fromJson("{\n"
                        + "    \"indexRules\": {\n"
                        + "      \"acme:Test\": {\n"
                        + "        \"properties\": {\n"
                        + "          \"def\": {\n"
                        + "            \"function\": \"upper(test)\",\n"
                        + "            \"boost\": 1.2\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }", true)).toString();
        assertEquals("{\n"
                + "  \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"indexRules\": {\n"
                + "    \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "    \"acme:Test\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"properties\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"abc\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"function\": \"upper(test)\",\n"
                + "          \"boost\": 1.2\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }

    @Test
    public void boost() {
        // - "analyzed" must not be overwritten
        // - "ordered" is added
        // - "boost" is overwritten
        String merged = DiffIndexMerger.processMerge(JsonObject.fromJson("{\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"acme:Test\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"abc\": {\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"analyzed\": true,\n"
                + "            \"boost\": 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "", true), JsonObject.fromJson("{\n"
                        + "    \"indexRules\": {\n"
                        + "      \"acme:Test\": {\n"
                        + "        \"properties\": {\n"
                        + "          \"abc\": {\n"
                        + "            \"analyzed\": false,\n"
                        + "            \"ordered\": true,\n"
                        + "            \"boost\": 1.2\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }", true)).toString();
        assertEquals("{\n"
                + "  \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"indexRules\": {\n"
                + "    \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "    \"acme:Test\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"properties\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"abc\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"analyzed\": true,\n"
                + "          \"boost\": 1.2,\n"
                + "          \"ordered\": true\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }
}
