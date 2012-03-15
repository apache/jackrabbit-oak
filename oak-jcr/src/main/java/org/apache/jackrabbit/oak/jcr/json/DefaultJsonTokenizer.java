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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This JSON tokenizer operates on a string as its input. For maximal performance
 * it <em>does not</em> unescape JSON string values.
 * Use {@link JsonValue#unescape(String)} to unescape the text of {@link Token}s
 * of type {@link Type#STRING}.
 * 
 * @see UnescapingJsonTokenizer
 */
public class DefaultJsonTokenizer extends JsonTokenizer {
    private final String json;

    private int pos;

    /**
     * Create a tokenizer for the given input string
     * @param json
     */
    public DefaultJsonTokenizer(String json) {
        this.json = json;
    }

    /**
     * @see JsonTokenizer#JsonTokenizer(JsonTokenizer)
     */
    protected DefaultJsonTokenizer(DefaultJsonTokenizer tokenizer) {
        super(tokenizer);
        json = tokenizer.json;
        pos = tokenizer.pos;
    }

    @Override
    protected Token nextToken() {
        skipWhiteSpace();
        if (pos >= json.length()) {
            return createToken(Type.EOF, "", pos);
        }

        switch (json.charAt(pos)) {
            case '{': return createToken(Type.BEGIN_OBJECT, "{", pos++);
            case '}': return createToken(Type.END_OBJECT, "}", pos++);
            case '[': return createToken(Type.BEGIN_ARRAY, "[", pos++);
            case ']': return createToken(Type.END_ARRAY, "]", pos++);
            case ':': return createToken(Type.COLON, ":", pos++);
            case ',': return createToken(Type.COMMA, ",", pos++);
            case 't': return readLiteral(Type.TRUE, "true");
            case 'f': return readLiteral(Type.FALSE, "false");
            case 'n': return readLiteral(Type.NULL, "null");
            case '"': return readString();
            default:  return isNumber() ? readNumber() : readUnknown();
        }
    }

    @Override
    public int pos() {
        return peek().pos();
    }

    @Override
    public void setPos(int pos) {
        currentToken = null;
        this.pos = pos;
    }

    @Override
    public String toString() {
        return (currentToken == null ? "" : currentToken) + " " + json.substring(pos);
    }

    @Override
    public DefaultJsonTokenizer copy() {
        return new DefaultJsonTokenizer(this);
    }

    //------------------------------------------< protected >---

    /**
     * Advance {@link #pos()} until the current character is not a
     * whitespace character.
     */
    protected void skipWhiteSpace() {
        while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
            pos++;
        }
    }

    /**
     * Factory method for creating {@link Token}s
     * @param type
     * @param text
     * @param pos
     * @return a new token
     */
    protected Token createToken(Type type, String text, int pos) {
        return new Token(type, text, pos);
    }

    /**
     * Read the literal {@code text} and create a token of the given {@code type}
     * @param type
     * @param text
     * @return a new token
     * @throws ParseException  if {@code text} cannot be read at the current position
     */
    protected Token readLiteral(Type type, String text) {
        if (json.substring(pos).startsWith(text)) {
            Token token = createToken(type, text, pos);
            pos += text.length();
            return token;
        }
        else {
            throw new ParseException(pos, "Expected '" + text + ",' found: " + excerpt(json, pos, 40));
        }
    }

    /**
     * Read a JSON string and create a {@link Token.Type#STRING} token.
     * @return a new token
     * @throws ParseException  if no string can be read at the current position
     */
    protected Token readString() {
        int i;
        boolean found = false;
        boolean even = true;

        // starting at pos + 1, find index i of the first quote character in json which
        // is preceded by an even number of backslash characters
        for (i = pos + 1; i < json.length() && !(found = json.charAt(i) == '"' && even); i++) {
            even = json.charAt(i) != '\\' || !even;
        }

        if (found) {
            Token token = createToken(Type.STRING, json.substring(pos + 1, i), pos);
            pos = i + 1;
            return token;
        }
        else {
            throw new ParseException(pos, "Expected string, found. " + excerpt(json, pos, 40));
        }
    }
    
    private static final Pattern NUMBER_PATTERN = Pattern.compile(
            "(\\+|-)?(\\d+)((\\.)(\\d+))?(((e|E)(\\+|-)?)(\\d+))?");

    /**
     * Read a JSON number and create a {@link Token.Type#NUMBER} token.
     * @return a new token
     * @throws ParseException  if no number can be read at the current position
     */
    protected Token readNumber() {
        Matcher matcher = NUMBER_PATTERN.matcher(json.substring(pos));
        if (matcher.lookingAt()) {
            Token token = createToken(Type.NUMBER, matcher.group(), pos);
            pos += matcher.end();
            return token;
        }
        else {
            throw new ParseException(pos, "Expected number, found. " + excerpt(json, pos, 40));
        }
    }

    /**
     * Read from the current position until a new token starts and create a
     * {@link Token.Type#UNKNOWN} token.
     * @return a new token
     */
    protected Token readUnknown() {
        int start = pos++;
        while (pos < json.length() && "{}[]:,tfn+-0123456789\" ".indexOf(json.charAt(pos)) == -1)  {
            pos++;
        }
        return new Token(Type.UNKNOWN, json.substring(start, pos), start);
    }

    //------------------------------------------< private >---

    private boolean isNumber() {
        // true if first character is a digit or a sign and second character is a digit
        char first = json.charAt(pos);
        return !(!Character.isDigit(first) &&
                ('+' != first && '-' != first || pos + 1 >= json.length() ||
                 !Character.isDigit(json.charAt(pos + 1))));
    }

    private static String excerpt(String string, int pos, int len) {
        return string.substring(pos, Math.min(string.length(), pos + len)) + "...";
    }

}
