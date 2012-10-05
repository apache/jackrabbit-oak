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

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Type;

public class StringsPropertyState extends MultiPropertyState<String> {
    protected StringsPropertyState(String name, List<String> values) {
        super(name, values);
    }

    @Override
    protected Iterable<String> getStrings() {
        return values;
    }

    @Override
    protected String getString(int index) {
        return values.get(index);
    }

    @Override
    protected Iterable<Boolean> getBooleans() {
        return Iterables.transform(values, new Function<String, Boolean>() {
            @Override
            public Boolean apply(String value) {
                return Boolean.parseBoolean(value);
            }
        });
    }

    @Override
    protected boolean getBoolean(int index) {
        return Boolean.parseBoolean(getString(index));
    }

    @Override
    public Type<?> getType() {
        return Type.STRINGS;
    }
}
