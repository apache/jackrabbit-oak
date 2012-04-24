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
package org.apache.jackrabbit.oak.kernel;

import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.api.CoreValue;

class KernelPropertyState extends AbstractPropertyState {
    private final String name;
    private final CoreValue value;
    private final List<CoreValue> values;

    public KernelPropertyState(String name, CoreValue value) {
        this.name = name;
        this.value = value;
        this.values = null;
    }

    public KernelPropertyState(String name, List<CoreValue> values) {
        this.name = name;
        this.value = null;
        this.values = Collections.unmodifiableList(values);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isArray() {
        return value == null;
    }
    
    @Override
    public CoreValue getValue() {
        return value;
    }

    @Override
    public Iterable<CoreValue> getValues() {
        return values;
    }

}
