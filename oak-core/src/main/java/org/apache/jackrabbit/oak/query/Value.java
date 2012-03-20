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
package org.apache.jackrabbit.oak.query;

import java.math.BigDecimal;

public class Value implements Comparable<Value> {

    private final Object value;
    private final int type;

    public Value(Object value, int type) {
        this.value = value;
        this.type = type;
    }

    public String getString() {
        return value.toString();
    }

    public int getType() {
        return type;
    }

    public long getLong() {
        // TODO convert?
        return ((Long) value).longValue();
    }

    public double getDouble() {
        // TODO convert?
        return ((Double) value).doubleValue();
    }

    public boolean getBoolean() {
        // TODO convert?
        return ((Boolean) value).booleanValue();
    }

    public BigDecimal getDecimal() {
        // TODO convert?
        return (BigDecimal) value;
    }

    public String getBinary() {
        // TODO convert?
        return value.toString();
    }

    public String getDate() {
        // TODO convert?
        return value.toString();
    }

    @Override
    public int hashCode() {
        return type ^ value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Value)) {
            return false;
        }
        Value v = (Value) o;
        return type == v.type && value.equals(v.value);
    }

    @Override
    public int compareTo(Value o) {
        if (this == o) {
            return 0;
        }
        if (type != o.type) {
            // TODO convert?
            return type - o.type;
        }
        switch (type) {
        case PropertyType.LONG:
            return ((Long) value).compareTo((Long) o.value);
        case PropertyType.DOUBLE:
            return ((Double) value).compareTo((Double) o.value);
        case PropertyType.DECIMAL:
            return ((BigDecimal) value).compareTo((BigDecimal) o.value);
        case PropertyType.BOOLEAN:
            return ((Boolean) value).compareTo((Boolean) o.value);
        }
        return value.toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return getString();
    }

}
