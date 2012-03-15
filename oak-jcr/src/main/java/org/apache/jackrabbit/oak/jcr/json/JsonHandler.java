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

/**
 * Handler for semantic actions of a {@link JsonParser}.
 * This class provides a handler which fully parses a JSON
 * document by recursive decent without executing any actions.
 * <p/>
 * Override this class to add semantic actions as needed. 
 */
public class JsonHandler {

    /**
     * Default instance which can be used to skip any part of a
     * JSON document. 
     */
    public static final JsonHandler INSTANCE = new JsonHandler();

    /**
     * A primitive JSON value (ATOM) has been parsed.
     * @param key
     * @param value
     */
    public void atom(Token key, Token value) { }

    /**
     * A COMMA has been parsed
     * @param token
     */
    public void comma(Token token) { }

    /**
     * Parser PAIR. This implementation simply delegates back
     * to {@link JsonParser#parsePair(JsonTokenizer)}
     * 
     * @param parser
     * @param tokenizer
     */
    public void pair(JsonParser parser, JsonTokenizer tokenizer) {
        parser.parsePair(tokenizer);
    }

    /**
     * Parser OBJECT. This implementation simply delegates back
     * to {@link JsonParser#parseObject(JsonTokenizer)}
     *
     * @param parser
     * @param key
     * @param tokenizer
     */
    public void object(JsonParser parser, Token key, JsonTokenizer tokenizer) {
        parser.parseObject(tokenizer);
    }

    /**
     * Parser ARRAY. This implementation simply delegates back
     * to {@link JsonParser#parseArray(JsonTokenizer)} 
     *
     * @param parser
     * @param key
     * @param tokenizer
     */
    public void array(JsonParser parser, Token key, JsonTokenizer tokenizer) {
        parser.parseArray(tokenizer);
    }
}
