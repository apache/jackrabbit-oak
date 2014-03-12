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

/**
 * A tokenizer for Json and Jsop strings.
 */
public class JsopTokenizer implements JsopReader {

    private static final String[] TYPE = {
            "end", "string", "number", "true", "false", "null", "error"
    };

    private final String jsop;
    private final int length;
    private int lastPos;
    private int pos;
    private int currentType;
    private boolean currentEscaped;
    private String currentToken;
    private int lastType;
    private String lastToken;
    private boolean lastEscaped;

    public JsopTokenizer(String json, int pos) {
        this.jsop = json;
        this.length = json.length();
        this.pos = pos;
        read();
    }

    public JsopTokenizer(String json) {
        this(json, 0);
    }

    @Override
    public void resetReader() {
        pos = 0;
        read();
    }

    @Override
    public String toString() {
        return jsop;
    }

    /**
     * Get the token type of the last token.
     *
     * @return the token type
     */
    @Override
    public int getTokenType() {
        return lastType;
    }

    /**
     * Get the last token value if the the token type was STRING or NUMBER. For
     * STRING, the text is decoded; for NUMBER, it is returned as parsed. In all
     * other cases the result is undefined.
     *
     * @return the token
     */
    @Override
    public String getToken() {
        if (lastType > COMMENT) {
            return String.valueOf((char) lastType);
        }
        return lastEscaped ? decode(lastToken) : lastToken;
    }

    /**
     * Get the last encoded (raw) string, including escape sequences.
     *
     * @return the encoded string
     */
    public String getEscapedToken() {
        return lastToken;
    }

    /**
     * Read a token which must match a given token type.
     *
     * @param type the token type
     * @return the token (a null object when reading a null value)
     * @throws IllegalStateException if the token type doesn't match
     */
    @Override
    public String read(int type) {
        if (matches(type)) {
            return getToken();
        }
        throw getFormatException(jsop, pos, getTokenType(type));
    }

    private void skip(int type) {
        if (!matches(type)) {
            throw getFormatException(jsop, pos, getTokenType(type));
        }
    }

    /**
     * Read a string.
     *
     * @return the de-escaped string
     * @throws IllegalStateException if the token type doesn't match
     */
    @Override
    public String readString() {
        return read(STRING);
    }

    /**
     * Read a token which must match a given token type.
     *
     * @param type the token type
     * @return true if there was a match
     */
    @Override
    public boolean matches(int type) {
        if (currentType == type) {
            read();
            return true;
        }
        return false;
    }

    /**
     * Read a token and return the token type.
     *
     * @return the token type
     */
    @Override
    public int read() {
        lastPos = pos;
        lastType = currentType;
        lastToken = currentToken;
        lastEscaped = currentEscaped;
        try {
            currentType = readToken();
        } catch (IllegalArgumentException e) {
            currentType = ERROR;
            currentToken = e.getMessage();
        } catch (StringIndexOutOfBoundsException e) {
            currentType = ERROR;
            currentToken = addAsterisk(jsop, pos);
        }
        return lastType;
    }

    private int readToken() {
        currentEscaped = false;
        char ch;
        while (true) {
            if (pos >= length) {
                return END;
            }
            ch = jsop.charAt(pos);
            if (ch > ' ') {
                break;
            }
            pos++;
        }
        int start = pos++;
        switch (ch) {
            case '\"': {
                while (true) {
                    ch = jsop.charAt(pos++);
                    if (ch == '\"') {
                        break;
                    } else if (ch == '\\') {
                        currentEscaped = true;
                        pos++;
                    }
                }
                currentToken = jsop.substring(start + 1, pos - 1);
                return STRING;
            }
            case '{':
            case '}':
            case '[':
            case ']':
            case '+':
            case ':':
            case ',':
            case '>':
            case '^':
            case '*':
            case '=':
            case ';':
                return ch;
            case '/': {
                ch = jsop.charAt(pos);
                if (ch != '*') {
                    return '/';
                }
                pos++;
                while (true) {
                    ch = jsop.charAt(pos++);
                    if (ch == '*' && jsop.charAt(pos) == '/') {
                        break;
                    }
                }
                currentToken = jsop.substring(start + 2, pos - 1);
                pos += 2;
                return COMMENT;
            }
            case '-':
                ch = jsop.charAt(pos);
                if (ch < '0' || ch > '9') {
                    // lookahead
                    return '-';
                }
                // else fall though
            default:
                if (ch >= '0' && ch <= '9') {
                    while (pos < length) {
                        ch = jsop.charAt(pos);
                        if (ch < '0' || ch > '9') {
                            break;
                        }
                        pos++;
                    }
                    if (ch == '.') {
                        pos++;
                        while (pos < length) {
                            ch = jsop.charAt(pos);
                            if (ch < '0' || ch > '9') {
                                break;
                            }
                            pos++;
                        }
                    }
                    if (ch == 'e' || ch == 'E') {
                        ch = jsop.charAt(++pos);
                        if (ch == '+' || ch == '-') {
                            ch = jsop.charAt(++pos);
                        }
                        while (pos < length) {
                            ch = jsop.charAt(pos);
                            if (ch < '0' || ch > '9') {
                                break;
                            }
                            pos++;
                        }
                    }
                    currentToken = jsop.substring(start, pos);
                    return NUMBER;
                } else if (ch >= 'a' && ch <= 'z') {
                    while (pos < length) {
                        ch = jsop.charAt(pos);
                        if ((ch < 'a' || ch > 'z') && ch != '_' && (ch < '0' || ch > '9')) {
                            break;
                        }
                        pos++;
                    }
                    String s = jsop.substring(start, pos);
                    if ("null".equals(s)) {
                        currentToken = null;
                        return NULL;
                    } else if ("true".equals(s)) {
                        currentToken = s;
                        return TRUE;
                    } else if ("false".equals(s)) {
                        currentToken = s;
                        return FALSE;
                    } else {
                        currentToken = s;
                        return IDENTIFIER;
                    }
                }
                throw getFormatException(jsop, pos);
        }
    }

    /**
     * Decode a quoted Json string.
     *
     * @param s the encoded string, with double quotes
     * @return the string
     */
    public static String decodeQuoted(String s) {
        if (s.length() < 2 || s.charAt(0) != '\"' || s.charAt(s.length() - 1) != '\"') {
            throw getFormatException(s, 0);
        }
        s = s.substring(1, s.length() - 1);
        return decode(s);
    }

    /**
     * Decode a Json string.
     *
     * @param s the encoded string, without double quotes
     * @return the string
     */
    public static String decode(String s) {
        if (s.indexOf('\\') < 0) {
            return s;
        }
        int length = s.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (i + 1 >= length) {
                    throw getFormatException(s, i);
                }
                c = s.charAt(++i);
                switch (c) {
                    case '"':
                        buff.append('"');
                        break;
                    case '\\':
                        buff.append('\\');
                        break;
                    case '/':
                        buff.append('/');
                        break;
                    case 'b':
                        buff.append('\b');
                        break;
                    case 'f':
                        buff.append('\f');
                        break;
                    case 'n':
                        buff.append('\n');
                        break;
                    case 'r':
                        buff.append('\r');
                        break;
                    case 't':
                        buff.append('\t');
                        break;
                    case 'u': {
                        try {
                            c = (char) (Integer.parseInt(s.substring(i + 1, i + 5), 16));
                        } catch (NumberFormatException e) {
                            throw getFormatException(s, i);
                        }
                        i += 4;
                        buff.append(c);
                        break;
                    }
                    default:
                        throw getFormatException(s, i);
                }
            } else {
                buff.append(c);
            }
        }
        return buff.toString();
    }

    private static String getTokenType(int type) {
        return type <= COMMENT ? TYPE[type] : "'" + (char) type + "'";
    }

    private static IllegalArgumentException getFormatException(String s, int i, String expected) {
        return new IllegalArgumentException(addAsterisk(s, i) + " expected: " + expected);
    }

    private static IllegalArgumentException getFormatException(String s, int i) {
        return new IllegalArgumentException(addAsterisk(s, i));
    }

    /**
     * Add an asterisk ('[*]') at the given position. This format is used to
     * show where parsing failed in a statement.
     *
     * @param s the text
     * @param index the position
     * @return the text with asterisk
     */
    private static String addAsterisk(String s, int index) {
        if (s != null) {
            index = Math.min(index, s.length());
            s = s.substring(0, index) + "[*]" + s.substring(index);
        }
        return s;
    }

    /**
     * Read a value and return the raw Json representation. This includes arrays
     * and nested arrays.
     *
     * @return the Json representation of the value
     */
    @Override
    public String readRawValue() {
        int start = lastPos;
        while (start < length && jsop.charAt(start) <= ' ') {
            start++;
        }
        skipRawValue();
        return jsop.substring(start, lastPos);
    }

    private void skipRawValue() {
        switch (currentType) {
            case '[': {
                int level = 0;
                while (true) {
                    if (matches(']')) {
                        if (--level == 0) {
                            break;
                        }
                    } else if (matches('[')) {
                        level++;
                    } else if (matches(END)) {
                        throw getFormatException(jsop, pos, "value");
                    } else {
                        read();
                    }
                }
                break;
            }
            case '{':
                read();
                if (!matches('}')) {
                    do {
                        skip(STRING);
                        read(':');
                        skipRawValue();
                    } while (matches(','));
                    read('}');
                }
                break;
            case NULL:
            case NUMBER:
            case TRUE:
            case FALSE:
            case COMMENT:
            case STRING:
            case IDENTIFIER:
                read();
                break;
            default:
                throw getFormatException(jsop, pos, "value");
        }
    }

    public int getPos() {
        return pos;
    }

    public int getLastPos() {
        return lastPos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

}
