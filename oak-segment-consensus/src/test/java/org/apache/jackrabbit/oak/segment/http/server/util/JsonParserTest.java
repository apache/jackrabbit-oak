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
package org.apache.jackrabbit.oak.segment.http.server.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JsonParserTest {

    @Test
    public void extractFieldDecodesEscapedStringValues() {
        String json = "{\"clientId\":\"a\\\"b\\\\c\\nd\\u0001e \\u00e9 \\ud83d\\ude00\",\"clientUrl\" : \"http://x/\\\"\"}";
        assertEquals("a\"b\\c\nd\u0001e \u00e9 \uD83D\uDE00", JsonParser.extractField(json, "clientId"));
        assertEquals("http://x/\"", JsonParser.extractField(json, "clientUrl"));
    }

    @Test
    public void extractFieldKeepsUnquotedAndMissingBehaviour() {
        String json = "{\"term\": 12,\"isLeader\":true,\"leader\":null}";
        assertEquals("12", JsonParser.extractField(json, "term"));
        assertEquals("true", JsonParser.extractField(json, "isLeader"));
        assertEquals("null", JsonParser.extractField(json, "leader"));
        assertNull(JsonParser.extractField(json, "missing"));
        assertNull(JsonParser.extractField("{\"clientId\":\"unterminated", "clientId"));
    }
}
