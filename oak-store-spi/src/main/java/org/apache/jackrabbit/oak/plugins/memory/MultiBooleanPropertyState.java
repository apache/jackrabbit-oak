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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

import static org.apache.jackrabbit.oak.api.Type.BOOLEANS;

public class MultiBooleanPropertyState extends MultiPropertyState<Boolean> {
    public MultiBooleanPropertyState(String name, Iterable<Boolean> values) {
        super(name, values);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of booleans.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#BOOLEANS}
     */
    public static PropertyState booleanProperty(String name, Iterable<Boolean> values) {
        return new MultiBooleanPropertyState(name, values);
    }

    @Override
    public Converter getConverter(Boolean value) {
        return Conversions.convert(value);
    }

    @Override
    public Type<?> getType() {
        return BOOLEANS;
    }
}
