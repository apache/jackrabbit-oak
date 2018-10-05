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
package org.apache.jackrabbit.oak.query.ast;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.fulltext.LikePattern;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;

/**
 * The enumeration of all operators.
 */
public enum Operator {

    EQUAL("=") {
        @Override
        public boolean evaluate(PropertyValue p1, PropertyValue p2) {
            return PropertyValues.match(p1, p2);
        }
    },

    NOT_EQUAL("<>") {
        @Override
        public boolean evaluate(PropertyValue p1, PropertyValue p2) {
            return PropertyValues.notMatch(p1, p2);
        }
    },

    GREATER_THAN(">") {
        @Override
        public boolean evaluate(PropertyValue p1, PropertyValue p2) {
            return p1.compareTo(p2) > 0;
        }
    },

    GREATER_OR_EQUAL(">=") {
        @Override
        public boolean evaluate(PropertyValue p1, PropertyValue p2) {
            return p1.compareTo(p2) >= 0;
        }
    },

    LESS_THAN("<") {
        @Override
        public boolean evaluate(PropertyValue p1, PropertyValue p2) {
            return p1.compareTo(p2) < 0;
        }
    },

    LESS_OR_EQUAL("<=") {
        @Override
        public boolean evaluate(PropertyValue p1, PropertyValue p2) {
            return p1.compareTo(p2) <= 0;
        }
    },

    LIKE("like") {
        @Override
        public boolean evaluate(PropertyValue p1, PropertyValue p2) {
            LikePattern like = new LikePattern(p2.getValue(Type.STRING));
            for (String s : p1.getValue(Type.STRINGS)) {
                if (like.matches(s)) {
                    return true;
                }
            }
            return false;
        }
    };

    /**
     * The name of this operator.
     */
    private final String name;

    Operator(String name) {
        this.name = name;
    }

    /**
     * "operand2 always evaluates to a scalar value"
     *
     * for multi-valued properties: if any of the value matches, then return true
     */
    public abstract boolean evaluate(PropertyValue p1, PropertyValue p2);

    /**
     * Returns the name of this query operator.
     */
    @Override
    public String toString() {
        return name;
    }

}
