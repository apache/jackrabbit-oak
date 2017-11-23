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

package org.apache.jackrabbit.oak.plugins.index.property;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;

import static com.google.common.collect.Sets.newLinkedHashSet;

public final class ValuePatternUtil {
    /**
     * Get the longest prefix of restrictions on a property.
     *
     * @param filter the filter with all restrictions
     * @param property the property
     * @return the longest prefix, or null if none
     */
    @CheckForNull
    public static String getLongestPrefix(Filter filter, String property) {
        boolean first = false, last = false;
        List<String> list = new ArrayList<>();
        for(PropertyRestriction p : filter.getPropertyRestrictions(property)) {
            if (p.isLike) {
                continue;
            }
            if (p.first != null) {
                if (p.first.isArray()) {
                    return null;
                }
                list.add(p.first.getValue(Type.STRING));
                first = true;
            }
            if (p.last != null) {
                if (p.last.isArray()) {
                    return null;
                }
                list.add(p.last.getValue(Type.STRING));
                last = true;
            }
        }
        if (!first || !last) {
            return null;
        }
        String prefix = list.get(0);
        for (String s : list) {
            while (!s.startsWith(prefix)) {
                prefix = prefix.substring(0, prefix.length() - 1);
                if (prefix.isEmpty()) {
                    return null;
                }
            }
        }
        return prefix;
    }

    @CheckForNull
    public static Set<String> getAllValues(PropertyRestriction restriction){
        return getValues(restriction, ValuePattern.MATCH_ALL);
    }

    @CheckForNull
    public static Set<String> getValues(PropertyRestriction restriction, ValuePattern pattern) {
        if (restriction.firstIncluding
                && restriction.lastIncluding
                && restriction.first != null
                && restriction.first.equals(restriction.last)) {
            // "[property] = $value"
            return read(restriction.first, pattern);
        } else if (restriction.list != null) {
            // "[property] IN (...)
            Set<String> values = newLinkedHashSet(); // keep order for testing
            for (PropertyValue value : restriction.list) {
                values.addAll(read(value, pattern));
            }
            return values;
        } else {
            // "[property] is not null" or "[property] is null"
            return null;
        }
    }

    @CheckForNull
    public static Set<String> read(PropertyValue value, ValuePattern pattern) {
        if (value == null) {
            return null;
        }
        Set<String> values = new HashSet<String>();
        for (String v : value.getValue(Type.STRINGS)) {
            if (!pattern.matches(v)) {
                continue;
            }
            values.add(v);
        }
        return values;
    }
}
