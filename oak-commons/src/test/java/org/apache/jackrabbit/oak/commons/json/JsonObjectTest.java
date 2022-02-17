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
package org.apache.jackrabbit.oak.commons.json;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the JsonObject implementation.
 */
public class JsonObjectTest {

    @Test
    public void fromJson() {
        JsonObject a = JsonObject.fromJson(" { } ", false);
        assertEquals("{}", a.toString());
        JsonObject b = JsonObject.fromJson("{\"az\": 1, \"b\": [2, 3], \"c\": null, \"d\": {}}", true);
        assertEquals("{\n" + 
                "  \"az\": 1,\n" + 
                "  \"b\": [2, 3],\n" + 
                "  \"c\": null,\n" + 
                "  \"d\": {}\n" + 
                "}", b.toString());
        assertEquals("{az=1, b=[2, 3], c=null}", b.getProperties().toString());
        assertEquals("{d={}}", b.getChildren().toString());
    }

    @Test
    public void newObjectNotRespectingOrder() {
        JsonObject a = new JsonObject();
        // we test whether what we put in will come out,
        // but we don't test the order
        for (int i = 0; i < 100; i++) {
            a.getProperties().put("x" + i, "" + i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("" + i, a.getProperties().get("x" + i));
        }
    }

    @Test
    public void newObjectRespectingOrder() {
        JsonObject a = new JsonObject(true);
        a.getProperties().put("az", "1");
        a.getProperties().put("b", "2");
        // we expect it's a LinkedHashMap or similar
        assertEquals("{\n" + 
                "  \"az\": 1,\n" + 
                "  \"b\": 2\n" + 
                "}", a.toString());
        assertEquals("{az=1, b=2}", a.getProperties().toString());
    }

    @Test
    public void handlesNullChild() {
        JsonObject a = new JsonObject(true);
        a.getChildren().put("test", (JsonObject) null);
        assertEquals("{\n" +
                "  \"test\": null\n" +
                "}", a.toString());

    }

    @Test
    public void handlesParsingNullValue() {
        JsonObject b = JsonObject.fromJson("{\"key\": null}", true);
        assertEquals("{\n" +
                "  \"key\": null\n" +
                "}", b.toString());
    }

    @Test
    public void handlesSettingNullValue() {
        JsonObject b = new JsonObject(true);
        b.getProperties().put("key", null);
        assertEquals("{\n" +
                "  \"key\": null\n" +
                "}", b.toString());
    }
}
