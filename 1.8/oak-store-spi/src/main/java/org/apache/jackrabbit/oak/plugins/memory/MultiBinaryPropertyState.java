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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;

public class MultiBinaryPropertyState extends MultiPropertyState<Blob> {
    public MultiBinaryPropertyState(String name, Iterable<Blob> values) {
        super(name, values);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of {@link Blob}.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#BINARIES}
     */
    public static PropertyState binaryPropertyFromBlob(String name, Iterable<Blob> values) {
        return new MultiBinaryPropertyState(name, values);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of byte arrays.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#BINARIES}
     */
    public static PropertyState binaryPropertyFromArray(String name, Iterable<byte[]> values) {
        List<Blob> blobs = Lists.newArrayList();
        for (byte[] data : values) {
            blobs.add(new ArrayBasedBlob(data));
        }
        return new MultiBinaryPropertyState(name, blobs);
    }

    @Override
    public Converter getConverter(Blob value) {
        return Conversions.convert(value);
    }

    @Override
    public Type<?> getType() {
        return BINARIES;
    }
}
