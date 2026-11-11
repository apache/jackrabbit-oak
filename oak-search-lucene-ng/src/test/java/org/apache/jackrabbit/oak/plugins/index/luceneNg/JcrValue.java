/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

/**
 * A property value together with its JCR type, for test cases.
 */
record JcrValue(Object value, Type<?> type) {

    PropertyState property(String name) {
        return PropertyStates.createProperty(name, value, type);
    }

    @Override
    public String toString() {
        return type + " " + value;
    }

    static JcrValue jcrString(String value) {
        return new JcrValue(value, Type.STRING);
    }

    static JcrValue jcrStrings(String... values) {
        return new JcrValue(List.of(values), Type.STRINGS);
    }

    static JcrValue jcrLong(long value) {
        return new JcrValue(value, Type.LONG);
    }

    static JcrValue jcrLongs(Long... values) {
        return new JcrValue(List.of(values), Type.LONGS);
    }

    static JcrValue jcrDouble(double value) {
        return new JcrValue(value, Type.DOUBLE);
    }

    static JcrValue jcrDoubles(Double... values) {
        return new JcrValue(List.of(values), Type.DOUBLES);
    }

    static JcrValue jcrDate(String value) {
        return new JcrValue(value, Type.DATE);
    }

    static JcrValue jcrDates(String... values) {
        return new JcrValue(List.of(values), Type.DATES);
    }

    static JcrValue jcrBoolean(boolean value) {
        return new JcrValue(value, Type.BOOLEAN);
    }

    static JcrValue jcrBooleans(Boolean... values) {
        return new JcrValue(List.of(values), Type.BOOLEANS);
    }

    static JcrValue jcrName(String value) {
        return new JcrValue(value, Type.NAME);
    }

    static JcrValue jcrNames(String... values) {
        return new JcrValue(List.of(values), Type.NAMES);
    }

    static JcrValue jcrPath(String value) {
        return new JcrValue(value, Type.PATH);
    }

    static JcrValue jcrPaths(String... values) {
        return new JcrValue(List.of(values), Type.PATHS);
    }

    static JcrValue jcrReference(String value) {
        return new JcrValue(value, Type.REFERENCE);
    }

    static JcrValue jcrReferences(String... values) {
        return new JcrValue(List.of(values), Type.REFERENCES);
    }

    static JcrValue jcrWeakReference(String value) {
        return new JcrValue(value, Type.WEAKREFERENCE);
    }

    static JcrValue jcrWeakReferences(String... values) {
        return new JcrValue(List.of(values), Type.WEAKREFERENCES);
    }

    static JcrValue jcrUri(String value) {
        return new JcrValue(value, Type.URI);
    }

    static JcrValue jcrUris(String... values) {
        return new JcrValue(List.of(values), Type.URIS);
    }

    static JcrValue jcrDecimal(String value) {
        return new JcrValue(new BigDecimal(value), Type.DECIMAL);
    }

    static JcrValue jcrDecimals(String... values) {
        return new JcrValue(Arrays.stream(values).map(BigDecimal::new).collect(Collectors.toList()), Type.DECIMALS);
    }
}
