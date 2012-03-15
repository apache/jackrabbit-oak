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
 * Abstract base class for JSON tokenizers.
 * A JSON tokenizer breaks a stream of character into {@link Token}s. It has
 * a current {@link #pos() position} and methods for inspecting, reading and
 * skipping the token at the current position.
 */
public abstract class JsonTokenizer {

    /**
     * The current token which has been read ahead, if any.
     * {@code null} otherwise.
     */
    protected Token currentToken;

    /**
     * Copy constructor. To be used in conjunction with {@link #copy()}
     * @param tokenizer
     */
    protected JsonTokenizer(JsonTokenizer tokenizer) {
        currentToken = tokenizer.currentToken;
    }

    protected JsonTokenizer() { }

    /**
     * Returns the current token without advancing the {@link #pos() position}
     * @return  current token
     */
    public Token peek() {
        if (currentToken == null) {
            currentToken = nextToken();
        }

        return currentToken;
    }

    /**
     * @param type
     * @return  {@code true} if and only if the current token is of the given {@code type}
     */
    public boolean peek(Type type) {
        return peek().type() == type;
    }

    /**
     * Returns the current token and advances the {@link #pos() position}
     * @return  current token
     */
    public Token read() {
        if (currentToken == null) {
            return nextToken();
        }
        else {
            Token token = currentToken;
            currentToken = null;
            return token;
        }
    }

    /**
     * Returns the current token and advances the {@link #pos() position} if the token
     * is of the given {@code type}.
     * @param type
     * @return  current token
     * @throws ParseException  if the token is not of the given {@code type}.
     */
    public Token read(Type type) {
        Token token = peek();
        if (token.type() == type) {
            return read();
        }
        else {
            throw new ParseException(token.pos(), "Expected token type " + type + ", found: " + token); 
        }
    }

    /**
     * Advances the {@link #pos() position} if the token is of the given {@code type}.
     * @param type
     * @return  {@code true} if and only if the is token is of the given {@code type}.
     */
    public boolean skip(Type type) {
        if (peek(type)) {
            read();
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * @return the current position
     */
    public abstract int pos();

    /**
     * Set the current position
     * @param pos
     */
    public abstract void setPos(int pos);

    /**
     * Create a copy of this tokenizer with the same state. Implementations usually
     * create a new instance by calling the (overriden) {@link #JsonTokenizer(JsonTokenizer) copy constructor}.
     * @return copy of this tokenizer
     */
    public abstract JsonTokenizer copy();

    /**
     * Read the next token from the input and advance the current {@link #pos() positon}.
     * @return  next token
     */
    protected abstract Token nextToken();
}
