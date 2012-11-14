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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.value.Conversions.convert;

import java.util.Calendar;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

public class LongPropertyState extends SinglePropertyState<Long> {
    private final long value;
    private final Type<?> type;

    public LongPropertyState(@Nonnull String name, long value, Type<?> type) {
        super(name);
        this.value = value;
        this.type = type;
    }

    /**
     * Create a {@code PropertyState} from a long.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#LONG}
     */
    public static PropertyState createLongProperty(
            @Nonnull String name, long value) {
        return new LongPropertyState(name, value, Type.LONG);
    }

    /**
     * Create a {@code PropertyState} for a date value from a long.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#DATE}
     */
    public static PropertyState createDateProperty(
            @Nonnull String name, long value) {
        return new LongPropertyState(name, value, Type.DATE);
    }

    /**
     * Create a {@code PropertyState} for a date.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#DATE}
     */
    public static PropertyState createDateProperty(
            @Nonnull String name, @Nonnull Calendar value) {
        return new LongPropertyState(
                name, convert(checkNotNull(value)).toLong(), Type.DATE);
    }

    /**
     * Create a {@code PropertyState} for a date from a String.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#DATE}
     * @throws IllegalArgumentException if {@code value} is not a parseable to a date.
     */
    public static PropertyState createDateProperty(
            @Nonnull String name, @Nonnull String value) {
        return createDateProperty(
                name, convert(checkNotNull(value)).toCalendar());
    }

    @Override
    public Long getValue() {
        return value;
    }

    @Override
    public Converter getConverter() {
        if (type == Type.DATE) {
            return convert(convert(value).toCalendar());
        }
        else {
            return Conversions.convert(value);
        }
    }

    @Override
    public Type<?> getType() {
        return type;
    }
}
