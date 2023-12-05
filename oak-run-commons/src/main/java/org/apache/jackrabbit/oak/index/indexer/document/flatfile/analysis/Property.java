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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis;

import java.util.Arrays;

public class Property {

    String name;
    boolean multiple;
    ValueType type;
    String[] values;
    
    public Property(String key, ValueType type, String value) {
        this(key, ValueType.STRING, new String[] {value}, false);
    }

    public Property(String key, ValueType type, String[] values, boolean multiple) {
        this.name = key;
        this.type = type;
        this.values = values;
        this.multiple = multiple;
    }

    public static enum ValueType {
        NULL(0),
        STRING(1),
        BINARY(2),
        LONG(3),
        DOUBLE(4),
        DATE(5),
        BOOLEAN(6),
        NAME(7),
        PATH(8),
        REFERENCE(9),
        WEAKREFERENCE(10),
        URI(11),
        DECIMAL(12);
        
        private static ValueType[] LIST = ValueType.values();
        private final int ordinal;
        private ValueType(int ordinal) {
            this.ordinal = ordinal;
        }
        public int getOrdinal() {
            return ordinal;
        }
        public static ValueType byOrdinal(int ordinal) {
            return LIST[ordinal];
        }
    }

    public ValueType getType() {
        return type;
    }
    
    public String[] getValues() {
        return values;
    }
    
    public String toString() {
        return name + ": " + Arrays.toString(values);
    }

    public String getName() {
        return name;
    }

}
