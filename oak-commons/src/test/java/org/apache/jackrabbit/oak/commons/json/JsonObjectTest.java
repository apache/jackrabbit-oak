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

import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

/**
 * Unit tests for class {@link JsonObject}.
 *
 * @date 27.06.2017
 * @see JsonObject
 **/
public class JsonObjectTest {


    @Test
    public void testCreateThrowsIllegalArgumentExceptionOne() {

        JsopTokenizer jsopTokenizer = new JsopTokenizer("\"XHIG\":w@+v0I (");

        try {
            JsonObject.create(jsopTokenizer);
            fail("Expecting exception: IllegalArgumentException");
        } catch(IllegalArgumentException e) {
            assertEquals("\"XHIG\":w@[*]+v0I ( expected: '}'",e.getMessage());
            assertEquals(JsopTokenizer.class.getName(), e.getStackTrace()[0].getClassName());
        }

    }


    @Test
    public void testCreateWithNonNull() {

        JsopTokenizer jsopTokenizer = new JsopTokenizer("\"XHIN\":{@+p0I (");

        try {
            JsonObject.create(jsopTokenizer);
            fail("Expecting exception: IllegalArgumentException");
        } catch(IllegalArgumentException e) {
            assertEquals("\"XHIN\":{@[*]+p0I ( expected: string",e.getMessage());
            assertEquals(JsopTokenizer.class.getName(), e.getStackTrace()[0].getClassName());
        }

    }


    @Test
    public void testCreateThrowsIllegalArgumentExceptionTwo() {

        JsopTokenizer jsopTokenizer = new JsopTokenizer("\"X?IslG\":w,U@4v0I (");

        try {
            JsonObject.create(jsopTokenizer);
            fail("Expecting exception: IllegalArgumentException");
        } catch(IllegalArgumentException e) {
            assertEquals("\"X?IslG\":w,U[*]@4v0I ( expected: string",e.getMessage());
            assertEquals(JsopTokenizer.class.getName(), e.getStackTrace()[0].getClassName());
        }

    }


    @Test
    public void testCreate() {

        JsopTokenizer jsopTokenizer = new JsopTokenizer("}", 0);
        JsonObject jsonObject = JsonObject.create(jsopTokenizer);

        assertEquals(1, jsopTokenizer.getPos());
        assertEquals("}", jsopTokenizer.toString());

        assertEquals(1, jsopTokenizer.getLastPos());
        assertEquals(125, jsopTokenizer.getTokenType());

        assertNull(jsopTokenizer.getEscapedToken());
        assertNotNull(jsonObject);

    }


    @Test
    public void testGetProperties() {

        JsonObject jsonObject = new JsonObject();
        Map<String, String> map = jsonObject.getProperties();

        assertTrue(map.isEmpty());
        assertEquals(0, map.size());

    }


    @Test
    public void testToJsonOne() {

        JsonObject jsonObject = new JsonObject();
        JsopBuilder jsopBuilder = new JsopBuilder();
        jsonObject.toJson(jsopBuilder);

        assertEquals("{}", jsopBuilder.toString());

    }


    @Test
    public void testToJsonThree() {

        JsonObject jsonObject = new JsonObject();
        JsopBuilder jsopBuilder = new JsopBuilder();
        jsopBuilder.key("a");

        jsonObject.getChildren().put("a", new JsonObject());
        jsonObject.toJson(jsopBuilder);

        assertEquals("\"a\":{\"a\":{}}", jsopBuilder.toString());

    }


    @Test
    public void testGetChildren() {

        JsonObject jsonObject = new JsonObject();
        Map<String, JsonObject> map = jsonObject.getChildren();

        assertTrue(map.isEmpty());
        assertEquals(0, map.size());

    }


}