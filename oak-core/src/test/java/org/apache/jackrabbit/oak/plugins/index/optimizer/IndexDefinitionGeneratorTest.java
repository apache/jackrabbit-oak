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
    public void simple() {
        String def = IndexDefinitionGenerator.generateIndexDefinition("xpath",
                "/jcr:root/content//element(*, acme:test)[@test=1] option (index tag testTag)");
        assertEquals("{\n"
                + "  \"index\": {\n"
                + "    \"compatVersion\": 2,\n"
                + "    \"async\": \"async\",\n"
                + "    \"queryPaths\": [\"/content\"],\n"
                + "    \"includedPaths\": [\"/content\"],\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"evaluatePathRestrictions\": true,\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"tags\": [\"testTag\"],\n"
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

    @Test
    public void dotInPropertyNameRule() {
        assertEquals("testHello", IndexDefinitionBuilder.getPropertyRuleNameFromJcrProperty("test.hello"));
    }

    @Test
    public void dotInPropertyName() {
        String def = IndexDefinitionGenerator.generateIndexDefinition("xpath",
                "/jcr:root/var//element(*,slingevent:Job)[@event.job.topic = 'x' and not(@slingevent:finishedState)] order by @slingevent:created ascending");
        assertEquals("{\n"
                + "  \"index\": {\n"
                + "    \"compatVersion\": 2,\n"
                + "    \"async\": \"async\",\n"
                + "    \"queryPaths\": [\"/var\"],\n"
                + "    \"includedPaths\": [\"/var\"],\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"evaluatePathRestrictions\": true,\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"slingevent:Job\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"finishedState\": {\n"
                + "            \"name\": \"slingevent:finishedState\",\n"
                + "            \"propertyIndex\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"nullCheckEnabled\": true\n"
                + "          },\n"
                + "          \"eventJobTopic\": {\n"
                + "            \"name\": \"event.job.topic\",\n"
                + "            \"propertyIndex\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\"\n"
                + "          },\n"
                + "          \"created\": {\n"
                + "            \"name\": \"slingevent:created\",\n"
                + "            \"ordered\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", def);
    }

}
