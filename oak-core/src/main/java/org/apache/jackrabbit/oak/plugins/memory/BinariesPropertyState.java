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
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;

public class BinariesPropertyState extends MultiPropertyState<Blob> {

    protected BinariesPropertyState(String name, List<Blob> values) {
        super(name, values);
    }

    @Override
    protected Iterable<String> getStrings() {
        return Iterables.transform(values, new Function<Blob, String>() {
            @Override
            public String apply(Blob value) {
                return "<binary>";
            }
        });
    }

    @Override
    protected String getString(int index) {
        return "<binary>";
    }

    @Override
    protected Iterable<Blob> getBlobs() {
        return values;
    }

    @Override
    protected Blob getBlob(int index) {
        return values.get(index);
    }

    @Override
    public Type<?> getType() {
        return BINARIES;
    }
}
