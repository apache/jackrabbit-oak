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

package org.apache.jackrabbit.oak.jcr.value;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.Value;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.value.ValueFactoryImpl;

/**
 * Utility class for converting between internal value representation and JCR
 * values.
 * todo: needs refactoring. see OAK-16.
 */
public final class ValueConverter {
    private ValueConverter() {}

    public static CoreValue toCoreValue(String value, int propertyType, SessionDelegate sessionDelegate) throws ValueFormatException {
        return toCoreValue(sessionDelegate.getValueFactory().createValue(value, propertyType), sessionDelegate);
    }

    public static CoreValue toCoreValue(Value value, SessionDelegate sessionDelegate) {
        ValueFactoryImpl vf = sessionDelegate.getValueFactory();
        return vf.getCoreValue(value);
    }

    public static List<CoreValue> toCoreValues(String[] values, int propertyType, SessionDelegate sessionDelegate) throws ValueFormatException {
        Value[] vs = new Value[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = sessionDelegate.getValueFactory().createValue(values[i], propertyType);
        }
        return toCoreValues(vs, sessionDelegate);
    }

    public static List<CoreValue> toCoreValues(Value[] values, SessionDelegate sessionDelegate) {
        List<CoreValue> cvs = new ArrayList<CoreValue>(values.length);
        for (Value jcrValue : values) {
            if (jcrValue != null) {
                cvs.add(toCoreValue(jcrValue, sessionDelegate));
            }
        }
        return cvs;
    }

    public static Value toValue(CoreValue coreValue, SessionDelegate sessionDelegate) {
        return sessionDelegate.getValueFactory().createValue(coreValue);
    }

    public static Value[] toValues(Iterable<CoreValue> coreValues, SessionDelegate sessionDelegate) {
        List<Value> values = new ArrayList<Value>();
        for (CoreValue cv : coreValues) {
            values.add(toValue(cv, sessionDelegate));
        }
        return values.toArray(new Value[values.size()]);
    }
}
