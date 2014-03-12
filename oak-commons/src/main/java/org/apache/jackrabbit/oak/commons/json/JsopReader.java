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

import javax.annotation.CheckForNull;

/**
 * A reader for Json and Jsop strings.
 */
public interface JsopReader {

    /**
     * The token type that signals the end of the stream.
     */
    int END = 0;

    /**
     * The token type of a string value.
     */
    int STRING = 1;

    /**
     * The token type of a number value.
     */
    int NUMBER = 2;

    /**
     * The token type of the value "true".
     */
    int TRUE = 3;

    /**
     * The token type of the value "false".
     */
    int FALSE = 4;

    /**
     * The token type of "null".
     */
    int NULL = 5;

    /**
     * The token type of a parse error.
     */
    int ERROR = 6;

    /**
     * The token type of an identifier (an unquoted string), if supported by the reader.
     */
    int IDENTIFIER = 7;

    /**
     * The token type of a comment, if supported by the reader.
     */
    int COMMENT = 8;

    /**
     * Read a token which must match a given token type.
     *
     * @param type the token type
     * @return the token (null when reading a null value)
     * @throws IllegalStateException if the token type doesn't match
     */
    String read(int type);

    /**
     * Read a string.
     *
     * @return the de-escaped string (null when reading a null value)
     * @throws IllegalStateException if the token type doesn't match
     */
    @CheckForNull
    String readString();

    /**
     * Read a token and return the token type.
     *
     * @return the token type
     */
    int read();

    /**
     * Read a token which must match a given token type.
     *
     * @param type the token type
     * @return true if there was a match
     */
    boolean matches(int type);

    /**
     * Return the row (escaped) token.
     *
     * @return the escaped string (null when reading a null value)
     */
    @CheckForNull
    String readRawValue();

    /**
     * Get the last token value if the the token type was STRING or NUMBER. For
     * STRING, the text is decoded; for NUMBER, it is returned as parsed. In all
     * other cases the result is undefined.
     *
     * @return the token
     */
    @CheckForNull
    String getToken();

    /**
     * Get the token type of the last token. The token type is one of the known
     * types (END, STRING, NUMBER,...), or, for Jsop tags such as "+", "-",
     * it is the Unicode character code of the tag.
     *
     * @return the token type
     */
    int getTokenType();

    /**
     * Reset the position to 0, so that to restart reading.
     */
    void resetReader();

}
