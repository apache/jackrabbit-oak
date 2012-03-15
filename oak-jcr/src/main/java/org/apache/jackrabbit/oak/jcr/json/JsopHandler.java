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

package org.apache.jackrabbit.oak.jcr.json;

public class JsopHandler {

    public void add(Token path, JsonTokenizer value) {
        new JsonParser(JsonHandler.INSTANCE).parseObject(value);
    }

    public void add(Token path, Token value) { }
    public void add(Token path, Token[] values) { }
    public void remove(Token path) { }
    public void set(Token path, Token value) { }
    public void set(Token path, Token[] values) { }
    public void reorder(Token path, Token position, Token target) { }
    public void move(Token path, Token target) { }
    public void test(Token path, Token value) { }
    public void test(Token path, Token[] values) { }

    public void metaData(JsonTokenizer value) {
        new JsonParser(JsonHandler.INSTANCE).parseObject(value);
    }

    public void extension(char op, Token path, JsonTokenizer value) {
        new JsonParser(JsonHandler.INSTANCE).parseObject(value);
    }

    public void extension(char op, Token[] values) { }
    public void extension(char op, Token value) { }
}
