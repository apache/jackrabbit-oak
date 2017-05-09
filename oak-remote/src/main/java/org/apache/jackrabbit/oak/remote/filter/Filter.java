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

import java.util.regex.Pattern;

class Filter {

    private Pattern pattern;

    public Filter(String filter) {
        StringBuilder builder = new StringBuilder();

        int star = filter.indexOf('*');

        while (star != -1) {
            if (star > 0 && filter.charAt(star - 1) == '\\') {
                builder.append(Pattern.quote(filter.substring(0, star - 1)));
                builder.append(Pattern.quote("*"));
            } else {
                builder.append(Pattern.quote(filter.substring(0, star)));
                builder.append(".*");
            }
            filter = filter.substring(star + 1);
            star = filter.indexOf('*');
        }

        builder.append(Pattern.quote(filter));

        pattern = Pattern.compile(builder.toString());
    }

    public boolean matches(String name) {
        return pattern.matcher(name).matches();
    }

}