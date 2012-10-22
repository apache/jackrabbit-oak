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

import java.util.Calendar;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

public class LongsPropertyState extends MultiPropertyState<Long> {
    private final Type<?> type;

    private LongsPropertyState(String name, Iterable<Long> values, Type<?> type) {
        super(name, values);
        this.type = type;
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of longs.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#LONGS}
     */
    public static LongsPropertyState createLongsProperty(String name, Iterable<Long> values) {
        return new LongsPropertyState(name, Lists.newArrayList(values), Type.LONGS);
    }

    /**
     * Create a multi valued {@code PropertyState} of dates from a list of longs.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#DATES}
     */
    public static LongsPropertyState createDatesPropertyFromLong(String name, Iterable<Long> values) {
        return new LongsPropertyState(name, Lists.newArrayList(values), Type.DATES);
    }

    /**
     * Create a multi valued {@code PropertyState} of dates.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#DATES}
     */
    public static LongsPropertyState createDatesPropertyFromCalendar(String name, Iterable<Calendar> values) {
        List<Long> dates = Lists.newArrayList();
        for (Calendar v : values) {
            dates.add(Conversions.convert(v).toLong());
        }
        return new LongsPropertyState(name, dates, Type.DATES);
    }

    /**
     * Create a multi valued {@code PropertyState} of dates from a list of strings.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#DATES}
     * @throws IllegalArgumentException if one of the {@code values} is not a parseable to a date.
     */
    public static LongsPropertyState createDatesProperty(String name, Iterable<String> values) {
        List<Long> dates = Lists.newArrayList();
        for (String v : values) {
            dates.add(Conversions.convert(Conversions.convert(v).toCalendar()).toLong());
        }
        return new LongsPropertyState(name, dates, Type.DATES);
    }

    @Override
    public Converter getConverter(Long value) {
        if (type == Type.DATES) {
            return Conversions.convert(Conversions.convert(value).toCalendar());
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
