/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.merge;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.junit.Test;

/**
 * Test merging index definitions.
 */
public class IndexDefMergerTest {

    @Test
    public void merge() throws IOException, CommitFailedException {
        String s = readFromResource("merge.txt");
        JsonObject json = JsonObject.fromJson(s, true);
        for(JsonObject e : array(json.getProperties().get("tests"))) {
            merge(e);
        }
    }

    private void merge(JsonObject e) {
        JsonObject ancestor = e.getChildren().get("ancestor");
        JsonObject custom = e.getChildren().get("custom");
        JsonObject product = e.getChildren().get("product");
        try {
            JsonObject got = IndexDefMergerUtils.merge(
                    "", ancestor,
                    "/oak:index/test-1-custom-1", custom,
                    product, "/oak:index/test-2");
            JsonObject expected = e.getChildren().get("expected");
            assertEquals(expected.toString(), got.toString());
        } catch (UnsupportedOperationException e2) {
            String expected = e.getProperties().get("expected");
            assertEquals("" + expected, "\"" + e2.getMessage() + "\"");
        }
    }

    static String readFromResource(String resourceName) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(
                IndexDefMergerTest.class.getResourceAsStream(resourceName))) {
            StringBuilder buff = new StringBuilder();
            try (LineNumberReader l = new LineNumberReader(reader)) {
                while (true) {
                    String s = l.readLine();
                    if (s == null) {
                        break;
                    }
                    if (s.trim().startsWith("//")) {
                        // comment
                        continue;
                    }
                    buff.append(s);
                    buff.append('\n');
                }
            }
            return buff.toString();
        }
    }

    private static ArrayList<JsonObject> array(String json) {
        ArrayList<JsonObject> list = new ArrayList<>();
        JsopTokenizer tokenizer = new JsopTokenizer(json);
        tokenizer.read('[');
        while (!tokenizer.matches(']')) {
            tokenizer.read('{');
            JsonObject j = JsonObject.create(tokenizer, true);
            list.add(j);
            tokenizer.matches(',');
        }
        return list;
    }

}
