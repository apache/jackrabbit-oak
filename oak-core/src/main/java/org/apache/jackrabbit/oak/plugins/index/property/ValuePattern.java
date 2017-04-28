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
package org.apache.jackrabbit.oak.plugins.index.property;

import java.util.regex.Pattern;

/**
 * A value pattern.
 */
public class ValuePattern {
    
    private final Pattern pattern;
    
    public ValuePattern(String pattern) {
        Pattern p = pattern == null ? null : Pattern.compile(pattern);
        this.pattern = p;
    }
    
    public boolean matches(String v) {
        return pattern == null || pattern.matcher(v).matches();
    }
    
    public boolean matchesAll() {
        return pattern == null;
    }

}
