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

import static org.apache.jackrabbit.oak.api.Type.LONGS;

public class LongsPropertyState extends MultiPropertyState<Long> {

    protected LongsPropertyState(String name, List<Long> values) {
        super(name, values);
    }

    @Override
    protected Iterable<BigDecimal> getDecimals() {
        return Iterables.transform(values, new Function<Long, BigDecimal>() {
            @Override
            public BigDecimal apply(Long value) {
                return Conversions.convert(value).toDecimal();
            }
        });
    }

    @Override
    protected BigDecimal getDecimal(int index) {
        return Conversions.convert(values.get(index)).toDecimal();
    }

    @Override
    protected Iterable<Double> getDoubles() {
        return Iterables.transform(values, new Function<Long, Double>() {
            @Override
            public Double apply(Long value) {
                return Conversions.convert(value).toDouble();
            }
        });
    }

    @Override
    protected Iterable<String> getDates() {
        return Iterables.transform(values, new Function<Long, String>() {
            @Override
            public String apply(Long value) {
                Calendar calendar = Conversions.convert(value).toDate();
                return Conversions.convert(calendar).toString();
            }
        });
    }

    @Override
    protected double getDouble(int index) {
        return Conversions.convert(values.get(index)).toDouble();
    }

    @Override
    protected String getDate(int index) {
        Calendar calendar = Conversions.convert(values.get(index)).toDate();
        return Conversions.convert(calendar).toString();
    }

    @Override
    protected Iterable<Long> getLongs() {
        return values;
    }

    @Override
    protected long getLong(int index) {
        return values.get(index);
    }

    @Override
    protected Iterable<String> getStrings() {
        return Iterables.transform(values, new Function<Long, String>() {
            @Override
            public String apply(Long value) {
                return Conversions.convert(value).toString();
            }
        });
    }

    @Override
    protected String getString(int index) {
        return Conversions.convert(values.get(index)).toString();
    }

    @Override
    public Type<?> getType() {
        return LONGS;
    }
}
