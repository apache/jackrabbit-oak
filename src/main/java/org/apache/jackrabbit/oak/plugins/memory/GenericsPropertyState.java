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
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

import static com.google.common.base.Preconditions.checkArgument;

public class GenericsPropertyState extends MultiPropertyState<String> {
    private final Type<?> type;

    /**
     * @throws IllegalArgumentException if {@code type.isArray()} is {@code false}
     */
    protected GenericsPropertyState(String name, List<String> values, Type<?> type) {
        super(name, values);
        checkArgument(type.isArray());
        this.type = type;
    }

    @Override
    protected Iterable<String> getStrings() {
        return values;
    }

    @Override
    protected String getString(int index) {
        return values.get(index);
    }

    @Override
    protected Iterable<Long> getLongs() {
        if (type == Type.DATES) {
            return Iterables.transform(values, new Function<String, Long>() {
                @Override
                public Long apply(String value) {
                    Calendar calendar = Conversions.convert(value).toDate();
                    return Conversions.convert(calendar).toLong();
                }
            });
        }
        else {
            return super.getLongs();
        }
    }

    @Override
    protected long getLong(int index) {
        if (type == Type.DATES) {
            Calendar calendar = Conversions.convert(values.get(index)).toDate();
            return Conversions.convert(calendar).toLong();
        }
        else {
            return super.getLong(index);
        }
    }

    @Override
    protected Iterable<Double> getDoubles() {
        if (type == Type.DATES) {
            return Iterables.transform(values, new Function<String, Double>() {
                @Override
                public Double apply(String value) {
                    Calendar calendar = Conversions.convert(value).toDate();
                    return Conversions.convert(calendar).toDouble();
                }
            });
        }
        else {
            return super.getDoubles();
        }
    }

    @Override
    protected double getDouble(int index) {
        if (type == Type.DATES) {
            Calendar calendar = Conversions.convert(values.get(index)).toDate();
            return Conversions.convert(calendar).toDouble();
        }
        else {
            return super.getDouble(index);
        }
    }

    @Override
    protected Iterable<String> getDates() {
        if (type == Type.DATES) {
            return Iterables.transform(values, new Function<String, String>() {
                @Override
                public String apply(String value) {
                    Calendar calendar = Conversions.convert(value).toDate();
                    return Conversions.convert(calendar).toString();
                }
            });
        }
        else {
            return super.getDates();
        }
    }

    @Override
    protected String getDate(int index) {
        if (type == Type.DATES) {
            Calendar calendar = Conversions.convert(values.get(index)).toDate();
            return Conversions.convert(calendar).toString();
        }
        else {
            return super.getDate(index);
        }
    }

    @Override
    protected Iterable<BigDecimal> getDecimals() {
        if (type == Type.DATES) {
            return Iterables.transform(values, new Function<String, BigDecimal>() {
                @Override
                public BigDecimal apply(String value) {
                    Calendar calendar = Conversions.convert(value).toDate();
                    return Conversions.convert(calendar).toDecimal();
                }
            });
        }
        else {
            return super.getDecimals();
        }
    }

    @Override
    protected BigDecimal getDecimal(int index) {
        if (type == Type.DATES) {
            Calendar calendar = Conversions.convert(values.get(index)).toDate();
            return Conversions.convert(calendar).toDecimal();
        }
        else {
            return super.getDecimal(index);
        }
    }

    @Override
    public Type<?> getType() {
        return type;
    }
}
