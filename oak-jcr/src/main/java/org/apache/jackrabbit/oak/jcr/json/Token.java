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
 * A token represents the smallest lexical unit in a JSON document.
 * A token has a {@link Type type}, a {@link #text() text} and a
 * {@link #pos() position} which refers to its place in the originating
 * JSON document. Note that the position is <em>not</em> taken into account
 * for equality.
 */
public final class Token {
    private final Type type;
    private final String text;
    private final int pos;

    public enum Type {BEGIN_OBJECT, END_OBJECT, BEGIN_ARRAY, END_ARRAY, COLON, COMMA, EOF, TRUE,
        FALSE, NULL, STRING, NUMBER, UNKNOWN}

    public Token(Type type, String text, int pos) {
        this.type = type;
        this.text = text;
        this.pos = pos;
    }

    public Type type() {
        return type;
    }

    public String text() {
        return text;
    }

    public int pos() {
        return pos;
    }

    @Override
    public String toString() {
        return "Token[" + type + ", " + text + ", " + pos + ']';
    }

    @Override
    public int hashCode() {
        return 37 * (37 * (17 + type().hashCode()) + text().hashCode());
    }

    /**
     * Two tokens are equal if and only if their texts and their types
     * are equal.
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof Token) {
            Token that = (Token) other;
            return that.type == type && that.text.equals(text);
        }
        else {
            return false;
        }
    }
}
