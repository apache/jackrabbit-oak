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

package org.apache.jackrabbit.oak.remote.filter;

import java.util.HashSet;
import java.util.Set;

public class Filters {

    private Set<Filter> includes = new HashSet<Filter>();

    private Set<Filter> excludes = new HashSet<Filter>();

    public Filters(Set<String> filters) {
        if (filters == null) {
            throw new IllegalArgumentException("filter set is null");
        }

        for (String filter : filters) {
            if (filter == null) {
                throw new IllegalArgumentException("filter is null");
            }

            if (filter.length() == 0) {
                throw new IllegalArgumentException("include filter is an empty string");
            }

            if (filter.startsWith("-") && filter.length() == 1) {
                throw new IllegalArgumentException("exclude filter is an empty string");
            }

        }

        for (String filter : filters) {
            if (filter.startsWith("-")) {
                excludes.add(new Filter(filter.substring(1)));
            } else {
                includes.add(new Filter(filter));
            }
        }

        if (includes.isEmpty()) {
            includes.add(new Filter("*"));
        }
    }

    public boolean matches(String name) {
        for (Filter include : includes) {
            if (include.matches(name)) {
                for (Filter exclude : excludes) {
                    if (exclude.matches(name)) {
                        return false;
                    }
                }

                return true;
            }
        }

        return false;
    }
}