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

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Type;

import static org.apache.jackrabbit.oak.api.Type.BOOLEANS;

public class BooleansPropertyState extends MultiPropertyState {
    private final List<Boolean> values;

    protected BooleansPropertyState(String name, List<Boolean> values) {
        super(name);
        this.values = values;
    }

    @Override
    protected Iterable<String> getStrings() {
        return Iterables.transform(values, new Function<Boolean, String>() {
            @Override
            public String apply(Boolean value) {
                return Boolean.toString(value);
            }
        });
    }

    @Override
    protected String getString(int index) {
        return Boolean.toString(values.get(index));
    }

    @Override
    protected Iterable<Boolean> getBooleans() {
        return values;
    }

    @Override
    protected boolean getBoolean(int index) {
        return values.get(index);
    }

    @Override
    public int count() {
        return values.size();
    }

    @Override
    public Type<?> getType() {
        return BOOLEANS;
    }
}
