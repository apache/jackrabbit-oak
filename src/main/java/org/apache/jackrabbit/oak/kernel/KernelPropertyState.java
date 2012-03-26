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

import org.apache.jackrabbit.mk.json.JsonBuilder;
import org.apache.jackrabbit.mk.model.AbstractPropertyState;
import org.apache.jackrabbit.mk.model.Scalar;
import org.apache.jackrabbit.mk.model.Scalar.Type;

import java.util.Collections;
import java.util.List;

public class KernelPropertyState extends AbstractPropertyState { // fixme make package private

    private final String name;
    private final Scalar value;
    private final List<Scalar> values;

    public KernelPropertyState(String name, Scalar value) {
        this.name = name;
        this.value = value;
        this.values = null;
    }

    public KernelPropertyState(String name, List<Scalar> values) {
        this.name = name;
        this.value = null;
        this.values = Collections.unmodifiableList(values);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getEncodedValue() {
        if (value == null) {
            String sep = "";
            StringBuilder sb = new StringBuilder("[");
            for (Scalar s : values) {
                sb.append(sep);
                sep = ",";
                sb.append(encodeValue(s));
            }
            sb.append(']');
            return sb.toString();
        }
        else {
            return encodeValue(value);
        }
    }

    @Override
    public boolean isMultiValued() {
        return value == null;
    }
    
    @Override
    public Scalar getValue() {
        return value;
    }
    
    @Override
    public List<Scalar> getValues() {
        return values;
    }
    
    //------------------------------------------------------------< private >--- 
    
    private static String encodeValue(Scalar value) {
        if (value.getType() == Type.STRING) {
            return '"' + JsonBuilder.escape(value.getString()) + '"';
        }
        else {
            return value.getString();
        }
    }
}
