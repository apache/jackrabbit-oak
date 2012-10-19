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
package org.apache.jackrabbit.oak.plugins.memory;

import java.math.BigDecimal;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

import static org.apache.jackrabbit.oak.api.Type.DECIMAL;

public class DecimalPropertyState extends SinglePropertyState {
    private final BigDecimal value;

    protected DecimalPropertyState(String name, BigDecimal value) {
        super(name);
        this.value = value;
    }

    @Override
    public BigDecimal getDecimal() {
        return value;
    }

    @Override
    public double getDouble() {
        return Conversions.convert(value).toDouble();
    }

    @Override
    public long getLong() {
        return Conversions.convert(value).toLong();
    }

    @Override
    protected String getDate() {
        return Conversions.convert(value).toDate();
    }

    @Override
    public String getString() {
        return Conversions.convert(value).toString();
    }

    @Override
    public Type<?> getType() {
        return DECIMAL;
    }
}
