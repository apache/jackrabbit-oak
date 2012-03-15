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


import org.apache.jackrabbit.oak.jcr.json.Token.Type;

/**
 * A parser for the JSON format accepting the following grammar:
 *
 * <pre>
 * OBJECT    ::= { (PAIR (, PAIR)*)? }
 * PAIR      ::= STRING : VALUE
 * VALUE     ::= OBJECT | ARRAY | STRING | NUMBER | true | false | null
 * ARRAY     ::= [ (VALUE (, VALUE)*)? ]
 * </pre>
 *
 * Semantic actions are attached through a {@link JsonHandler} instance
 * which is passed to the constructor. For each of the above productions
 * the parser provides a corresponding method which take a {@link JsonTokenizer}
 * for reading the JSON input. These methods call the respective call back on the
 * {@code JsonHandler} for each of the constituents of the production.
 * <p/>
 * Note: In contrast to conventional parsers, this parser <em>does not</em>
 * recursively decent into nested structures (OBJECT, ARRAY and PAIR, that is).
 * Instead it calls the respective method on the {@code JsonHandler} which
 * is can use this parser instance or any other parser to continue parsing.
 *
 * @see <a href="http://www.json.org/">json.org</a>
 */
public final class JsonParser {
    private final JsonHandler jsonHandler;

    public JsonParser(JsonHandler jsonHandler) {
        this.jsonHandler = jsonHandler;
    }

    /**
     * Parses
     * <pre>
     * OBJECT ::= { (PAIR (, PAIR)*)? }
     * </pre>
     * Calls {@link JsonHandler#comma(Token)}
     * @param tokenizer
     * @throws ParseException
     */
    public void parseObject(JsonTokenizer tokenizer) {
        tokenizer.read(Type.BEGIN_OBJECT);

        if (tryParsePair(tokenizer)) {
            while (tokenizer.peek(Type.COMMA)) {
                jsonHandler.comma(tokenizer.read());
                if (!tryParsePair(tokenizer)) {
                    throw new ParseException(tokenizer.pos(),  "Expected pair, found: " + tokenizer.peek());
                }
            }
        }
        tokenizer.read(Type.END_OBJECT);
    }

    /**
     * Parses
     * <pre>
     * PAIR ::= STRING: VALUE
     * </pre>
     * @param tokenizer
     * @throws ParseException
     */
    public void parsePair(JsonTokenizer tokenizer) {
        if (!tokenizer.peek(Type.STRING)) {
            throw new ParseException(tokenizer.pos(), "Expected string, found: " + tokenizer.peek());
        }

        Token key = tokenizer.read();
        tokenizer.read(Type.COLON);
        parseValue(key, tokenizer);
    }

    /**
     * Parses
     * <pre>
     * VALUE ::= OBJECT | ARRAY | STRING | NUMBER | true | false | null
     * </pre>
     * Calls one of {@link JsonHandler#object(JsonParser, Token, JsonTokenizer)},
     * {@link JsonHandler#array(JsonParser, Token, JsonTokenizer)} and
     * {@link JsonHandler#atom(Token, Token)}
     * @param tokenizer
     * @throws ParseException
     */
    public void parseValue(Token key, JsonTokenizer tokenizer) {
        switch (tokenizer.peek().type()) {
            case BEGIN_OBJECT:
                jsonHandler.object(this, key, tokenizer);
                break;
            case BEGIN_ARRAY:
                jsonHandler.array(this, key, tokenizer);
                break;
            case STRING:
            case NUMBER:
            case TRUE:
            case FALSE:
            case NULL:
                jsonHandler.atom(key, tokenizer.read());
                break;
            default:
                throw new ParseException(tokenizer.pos(), "Expected value, found: " + tokenizer.peek());
        }
    }

    /**
     * Parses
     * <pre>
     * ARRAY ::= [ (VALUE (, VALUE)*)? ]
     * </pre>
     * Calls {@link JsonHandler#comma(Token)}
     * @param tokenizer
     * @throws ParseException
     */
    public void parseArray(JsonTokenizer tokenizer) {
        tokenizer.read(Type.BEGIN_ARRAY);

        if (tryParseValue(tokenizer)) {
            while (tokenizer.peek(Type.COMMA)) {
                jsonHandler.comma(tokenizer.read());
                if (!tryParseValue(tokenizer)) {
                    throw new ParseException(tokenizer.pos(), "Expected value, found: " + tokenizer.peek()); 
                }
            }
        }
        tokenizer.read(Type.END_ARRAY);
    }

    //------------------------------------------------------------< private >---
    
    private boolean tryParsePair(JsonTokenizer tokenizer) {
        if (tokenizer.peek(Type.STRING)) {
            jsonHandler.pair(this, tokenizer);
            return true;
        }
        else {
            return false;
        }
    }

    private boolean tryParseValue(JsonTokenizer tokenizer) {
        if (tokenizer.peek(Type.END_ARRAY)) {
            return false;
        }
        else {
            parseValue(null, tokenizer);
            return true;
        }
    }

}
