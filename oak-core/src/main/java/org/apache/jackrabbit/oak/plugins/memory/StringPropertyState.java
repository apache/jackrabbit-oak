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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;

public class StringPropertyState extends SinglePropertyState<String> {
    private final String value;

    public StringPropertyState(@Nonnull String name, @Nonnull String value) {
        super(name);
        this.value = checkNotNull(value);
    }

    /**
     * Create a {@code PropertyState} from a string.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#STRING}
     */
    public static PropertyState stringProperty(
            @Nonnull String name, @Nonnull String value) {
        return new StringPropertyState(name, value);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Converter getConverter() {
        return Conversions.convert(value);
    }

    @Override
    public Type<?> getType() {
        return STRING;
    }

}
