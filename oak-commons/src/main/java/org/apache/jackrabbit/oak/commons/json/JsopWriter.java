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
 * A builder for Json and Json diff strings. It knows when a comma is needed. A
 * comma is appended before '{', '[', a value, or a key; but only if the last
 * appended token was '}', ']', or a value. There is no limit to the number of
 * nesting levels.
 */
public interface JsopWriter {

    /**
     * Append '['. A comma is appended first if needed.
     *
     * @return this
     */
    JsopWriter array();

    /**
     * Append '{'. A comma is appended first if needed.
     *
     * @return this
     */
    JsopWriter object();

    /**
     * Append the key (in quotes) plus a colon. A comma is appended first if
     * needed.
     *
     * @param key the key
     * @return this
     */
    JsopWriter key(String key);

    /**
     * Append a string or null. A comma is appended first if needed.
     *
     * @param value the value
     * @return this
     */
    JsopWriter value(String value);

    /**
     * Append an already encoded value. A comma is appended first if needed.
     *
     * @param raw the value
     * @return this
     */
    JsopWriter encodedValue(String raw);

    /**
     * Append '}'.
     *
     * @return this
     */
    JsopWriter endObject();

    /**
     * Append ']'.
     *
     * @return this
     */
    JsopWriter endArray();

    /**
     * Append a Jsop tag character.
     *
     * @param tag the string to append
     * @return this
     */
    JsopWriter tag(char tag);

    /**
     * Append all entries of the given writer.
     *
     * @param diff the writer
     * @return this
     */
    JsopWriter append(JsopWriter diff);

    /**
     * Append a number. A comma is appended first if needed.
     *
     * @param x the value
     * @return this
     */
    JsopWriter value(long x);

    /**
     * Append the boolean value 'true' or 'false'. A comma is appended first if
     * needed.
     *
     * @param b the value
     * @return this
     */
    JsopWriter value(boolean b);

    /**
     * Append a newline character.
     *
     * @return this
     */
    JsopWriter newline();

    /**
     * Resets this instance, so that all data is discarded.
     */
    void resetWriter();

    /**
     * Set the line length, after which a newline is added (to improve
     * readability).
     *
     * @param length the length
     */
    void setLineLength(int length);

}
