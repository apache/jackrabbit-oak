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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class IndexDefinitionGeneratorTest {

    @Test
    public void test() {
        String def = IndexDefinitionGenerator.generateIndexDefinition("xpath", "/jcr:root/content//element(*, acme:test)[@test=1]");
        assertEquals("{\n"
                + "  \"index\": {\n"
                + "    \"compatVersion\": 2,\n"
                + "    \"async\": \"async\",\n"
                + "    \"queryPaths\": [\"/content\"],\n"
                + "    \"includedPaths\": [\"/content\"],\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"evaluatePathRestrictions\": true,\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"acme:test\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"test\": {\n"
                + "            \"name\": \"test\",\n"
                + "            \"propertyIndex\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", def);
    }
}
