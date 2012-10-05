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
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Type;

import static org.apache.jackrabbit.oak.api.Type.DOUBLES;

public class DoublesPropertyState extends MultiPropertyState {
    private final List<Double> values;

    protected DoublesPropertyState(String name, List<Double>values) {
        super(name);
        this.values = values;
    }

    @Override
    protected Iterable<BigDecimal> getDecimals() {
        return Iterables.transform(values, new Function<Double, BigDecimal>() {
            @Override
            public BigDecimal apply(Double value) {
                return new BigDecimal(value);
            }
        });
    }

    @Override
    protected BigDecimal getDecimal(int index) {
        return new BigDecimal(values.get(index));
    }

    @Override
    protected Iterable<Double> getDoubles() {
        return values;
    }

    @Override
    protected double getDouble(int index) {
        return values.get(index);
    }

    @Override
    protected Iterable<Long> getLongs() {
        return Iterables.transform(values, new Function<Double, Long>() {
            @Override
            public Long apply(Double value) {
                return value.longValue();
            }
        });
    }

    @Override
    protected long getLong(int index) {
        return values.get(index).longValue();
    }

    @Override
    protected Iterable<String> getStrings() {
        return Iterables.transform(values, new Function<Double, String>() {
            @Override
            public String apply(Double value) {
                return String.valueOf(value);
            }
        });
    }

    @Override
    protected String getString(int index) {
        return String.valueOf(values.get(index));
    }

    @Override
    public int count() {
        return values.size();
    }

    @Override
    public Type<?> getType() {
        return DOUBLES;
    }
}
