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

import java.util.ArrayList;
import java.util.List;

/*
 * DIFFS     ::= DIFF*
 * DIFF      ::= ADD | SET | REMOVE | MOVE | TEST | METADATA | EXTENSION
 * ADD       ::= + STRING : (OBJECT | ATOM | ARRAY)
 * SET       ::= ^ STRING : ATOM | ARRAY
 * REMOVE    ::= - STRING
 * MOVE      ::= > STRING : (STRING | { STRING : STRING })
 * TEST      ::= = STRING : ATOM | ARRAY
 * METADATA  ::= @ OBJECT
 * EXTENSION ::= OP STRING ":" (OBJECT | ATOM | ARRAY)
 */
public class JsopParser {
    private final JsopHandler jsopHandler;

    public JsopParser(JsopHandler jsopHandler) {
        this.jsopHandler = jsopHandler;
    }

    /* DIFFS    ::= DIFF* */
    public void parseJsop(JsonTokenizer tokenizer) {
        for (Token token = tokenizer.peek(); token.type() != Type.EOF; token = tokenizer.peek()) {
            parseDiff(tokenizer);
        }
    }

    /* DIFF    ::= ADD | SET | REMOVE | MOVE */
    public void parseDiff(JsonTokenizer tokenizer) {
        Token token = tokenizer.read(Type.UNKNOWN);
        String text = token.text();
        if (text.length() != 1) {
            throw new ParseException(token.pos(), "Expected one of +, -, ^ or >. Found: " + token);
        }

        switch (text.charAt(0)) {
            case '+':
                parseAdd(tokenizer);
                break;
            case '-':
                parseRemove(tokenizer);
                break;
            case '^':
                parseSet(tokenizer);
                break;
            case '>':
                parseMove(tokenizer);
                break;
            case '=':
                parseTest(tokenizer);
                break;
            case '@':
                parseMetadata(tokenizer);
                break;
            default:
                parseExtension(text.charAt(0), tokenizer);
        }
    }

    /* ADD      ::= + STRING : (OBJECT | ATOM | ARRAY) */
    public void parseAdd(JsonTokenizer tokenizer) {
        Token path = tokenizer.read(Type.STRING);
        tokenizer.read(Type.COLON);
        switch (tokenizer.peek().type()) {
            case BEGIN_OBJECT:
                jsopHandler.add(path, tokenizer);
                break;
            case BEGIN_ARRAY:
                jsopHandler.add(path, parseArray(tokenizer));
                break;
            default:
                jsopHandler.add(path, parseAtom(tokenizer));
        }
    }

    /* REMOVE   ::= - STRING */
    public void parseRemove(JsonTokenizer tokenizer) {
        jsopHandler.remove(tokenizer.read(Type.STRING));
    }

    /* SET      ::= ^ STRING : ATOM | ARRAY*/
    public void parseSet(JsonTokenizer tokenizer) {
        Token path = tokenizer.read(Type.STRING);
        tokenizer.read(Type.COLON);
        Token token = tokenizer.peek();
        switch (token.type()) {
            case BEGIN_OBJECT:
                throw new ParseException(token.pos(), "Expected one of atom or array. Found: " + token);
            case BEGIN_ARRAY:
                jsopHandler.set(path, parseArray(tokenizer));
                break;
            default:
                jsopHandler.set(path, parseAtom(tokenizer));
        }
    }

    /* MOVE     ::= > STRING : (STRING | { STRING : STRING }) */
    public void parseMove(JsonTokenizer tokenizer) {
        Token path = tokenizer.read(Type.STRING);
        tokenizer.read(Type.COLON);
        if (tokenizer.peek(Type.BEGIN_OBJECT)) {
            tokenizer.read(Type.BEGIN_OBJECT);
            Token position = tokenizer.read(Type.STRING);
            tokenizer.read(Type.COLON);
            Token target = tokenizer.read(Type.STRING);
            tokenizer.read(Type.END_OBJECT);
            jsopHandler.reorder(path, position, target);
        }
        else {
            jsopHandler.move(path, tokenizer.read(Type.STRING));
        }
    }

    /* TEST     ::= = STRING : ATOM | ARRAY */
    public void parseTest(JsonTokenizer tokenizer) {
        Token path = tokenizer.read(Type.STRING);
        tokenizer.read(Type.COLON);
        Token token = tokenizer.peek();
        switch (token.type()) {
            case BEGIN_OBJECT:
                throw new ParseException(token.pos(), "Expected one of atom or array. Found: " + token);
            case BEGIN_ARRAY:
                jsopHandler.test(path, parseArray(tokenizer));
                break;
            default:
                jsopHandler.test(path, parseAtom(tokenizer));
        }
    }

    /* METADATA ::= @ OBJECT */
    public void parseMetadata(JsonTokenizer tokenizer) {
        jsopHandler.metaData(tokenizer);
    }

    /* EXTENSION ::= OP STRING ":" (OBJECT | ATOM | ARRAY) */
    public void parseExtension(char op, JsonTokenizer tokenizer) {
        Token path = tokenizer.read(Type.STRING);
        tokenizer.read(Type.COLON);
        switch (tokenizer.peek().type()) {
            case BEGIN_OBJECT:
                jsopHandler.extension(op, path, tokenizer);
                break;
            case BEGIN_ARRAY:
                jsopHandler.extension(op, parseArray(tokenizer));
                break;
            default:
                jsopHandler.extension(op, parseAtom(tokenizer));
        }
    }

    //------------------------------------------< private >---

    private static Token parseAtom(JsonTokenizer tokenizer) {
        Token token = tokenizer.peek();
        Type type = token.type();
        if (type != Type.TRUE && type != Type.FALSE && type != Type.NULL && type != Type.STRING && type != Type.NUMBER) {
            throw new ParseException(token.pos(), "Expected one of atom. Found: " + token);
        }
        return tokenizer.read();
    }

    private static Token[] parseArray(JsonTokenizer tokenizer) {
        final List<Token> values = new ArrayList<Token>();
        
        new JsonParser(new JsonHandler(){
            @Override
            public void atom(Token key, Token value) {
                values.add(value);
            }
        }).parseArray(tokenizer);
        
        return values.toArray(new Token[values.size()]);
    }

}
