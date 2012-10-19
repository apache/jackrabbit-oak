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
package org.apache.jackrabbit.oak.plugins.memory;

import java.math.BigDecimal;
import java.util.Calendar;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

public class LongPropertyState extends SinglePropertyState {
    private final long value;
    private final Type<?> type;

    private LongPropertyState(String name, long value, Type<?> type) {
        super(name);
        this.value = value;
        this.type = type;
    }

    public static LongPropertyState createLongProperty(String name, long value) {
        return new LongPropertyState(name, value, Type.LONG);
    }

    public static LongPropertyState createDateProperty(String name, long value) {
        return new LongPropertyState(name, value, Type.DATE);
    }

    public static LongPropertyState createDateProperty(String name, Calendar value) {
        return new LongPropertyState(name, Conversions.convert(value).toLong(), Type.DATE);
    }

    public static LongPropertyState createDateProperty(String name, String value) {
        return createDateProperty(name, Conversions.convert(value).toCalendar());
    }

    @Override
    public BigDecimal getDecimal() {
        return Conversions.convert(value).toDecimal();
    }

    @Override
    public double getDouble() {
        return Conversions.convert(value).toDouble();
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    protected String getDate() {
        return Conversions.convert(value).toDate();
    }

    @Override
    public String getString() {
        return type == Type.LONG
            ? Conversions.convert(value).toString()
            : getDate();
    }

    @Override
    public Type<?> getType() {
        return type;
    }
}
