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
package org.apache.jackrabbit.oak.plugins.index.elastic.util;

import static org.junit.Assert.assertEquals;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class ElasticIndexUtilsTest {

    @Test
    public void fieldName() {
        assertEquals("regular", ElasticIndexUtils.fieldName("regular"));
        assertEquals(":nodeName", ElasticIndexUtils.fieldName(":nodeName"));
        assertEquals("first|2e|name", ElasticIndexUtils.fieldName("first.name"));
        assertEquals("weird|5e|", ElasticIndexUtils.fieldName("weird^"));
        assertEquals("embedded_is_fine", ElasticIndexUtils.fieldName("embedded_is_fine"));
        assertEquals("|_id", ElasticIndexUtils.fieldName("_id"));
        assertEquals("|", ElasticIndexUtils.fieldName(""));
        assertEquals("| ", ElasticIndexUtils.fieldName(" "));
        assertEquals("||", ElasticIndexUtils.fieldName("|"));
        assertEquals("||test||", ElasticIndexUtils.fieldName("|test|"));
    }

    @Test
    public void randomFieldNames() {
        ElasticIndexUtils.propertyNameFromFieldName("");
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            StringBuilder buff = new StringBuilder();
            int len = 1 + r.nextInt(5);
            String chars = "|^._ 25ex";
            for (int j = 0; j < len; j++) {
                buff.append(chars.charAt(r.nextInt(chars.length())));
            }
            String p = buff.toString();
            String f = ElasticIndexUtils.fieldName(p);
            String p2 = ElasticIndexUtils.propertyNameFromFieldName(f);
            if (!p.equals(p2)) {
                p2 = ElasticIndexUtils.propertyNameFromFieldName(f);
                assertEquals(p, p2);
            }
            // just to make sure there are no exceptions (within some limits)
            ElasticIndexUtils.propertyNameFromFieldName(p);
        }
    }
    
    @Test
    public void idFromPath() {
        assertEquals("/content", ElasticIndexUtils.idFromPath("/content"));
        assertEquals("%40%0Bz%DF%B4%22%29%EF%BF%BD%EF%BF%BD%3Cfh%EF%BF%BD%27%EF%BF%BD%7E%EF%BF%BDM%EF%BF%BD%EF%BF%BD%EF%BF%BD%22I%EF%BF%BD%7C%EF%BF%BDGn%0A+%25", 
                URLEncoder.encode(ElasticIndexUtils.idFromPath("/content".repeat(100)),StandardCharsets.UTF_8));
    }
    
    @Test
    public void toByteArray() {
        assertEquals("[1.0, 0.1]",
                ElasticIndexUtils.toFloats(
                ElasticIndexUtils.toByteArray(List.of(1.0f, 0.1f))).toString());
        assertEquals("[-0.0, 0.0]",
                ElasticIndexUtils.toFloats(
                ElasticIndexUtils.toByteArray(List.of(-0.0f, 0.0f))).toString());
        assertEquals("[Infinity, -Infinity]",
                ElasticIndexUtils.toFloats(
                ElasticIndexUtils.toByteArray(List.of(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY))).toString());
        assertEquals("[NaN, 3.4028235E38]",
                ElasticIndexUtils.toFloats(
                ElasticIndexUtils.toByteArray(List.of(Float.NaN, Float.MAX_VALUE))).toString());
    }

}
