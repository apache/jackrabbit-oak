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
import java.util.Calendar;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

import static com.google.common.base.Preconditions.checkArgument;

public class GenericPropertyState extends SinglePropertyState {
    private final String value;
    private final Type<?> type;

    /**
     * @throws IllegalArgumentException if {@code type.isArray()} is {@code true}
     */
    protected GenericPropertyState(String name, String value, Type<?> type) {
        super(name);
        checkArgument(!type.isArray());
        this.value = value;
        this.type = type;
    }

    @Override
    protected String getString() {
        return value;
    }

    @Override
    protected long getLong() {
        if (type == Type.DATE) {
            Calendar calendar = Conversions.convert(value).toDate();
            return Conversions.convert(calendar).toLong();
        }
        else {
            return super.getLong();
        }
    }

    @Override
    protected double getDouble() {
        if (type == Type.DATE) {
            Calendar calendar = Conversions.convert(value).toDate();
            return Conversions.convert(calendar).toDouble();
        }
        else {
            return super.getDouble();
        }
    }

    @Override
    protected String getDate() {
        if (type == Type.DATE) {
            Calendar calendar = Conversions.convert(value).toDate();
            return Conversions.convert(calendar).toString();
        }
        else {
            return super.getDate();
        }
    }

    @Override
    protected BigDecimal getDecimal() {
        if (type == Type.DATE) {
            Calendar calendar = Conversions.convert(value).toDate();
            return Conversions.convert(calendar).toDecimal();
        }
        else {
            return super.getDecimal();
        }
    }

    @Override
    public Type<?> getType() {
        return type;
    }
}
