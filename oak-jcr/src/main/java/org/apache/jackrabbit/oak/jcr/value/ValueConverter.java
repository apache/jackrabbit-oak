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

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.jcr.SessionContext;

import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting between internal value representation and JCR
 * values.
 * todo: needs refactoring. see OAK-16.
 */
public final class ValueConverter {
    private ValueConverter() {}

    public static CoreValue toCoreValue(String value, int propertyType, SessionContext sessionContext) throws ValueFormatException {
        return toCoreValue(sessionContext.getValueFactory().createValue(value, propertyType), sessionContext);
    }

    public static CoreValue toCoreValue(Value value, SessionContext sessionContext) {
        ValueFactoryImpl vf = sessionContext.getValueFactory();
        return vf.getCoreValue(value);
    }

    public static List<CoreValue> toCoreValues(String[] values, int propertyType, SessionContext sessionContext) throws ValueFormatException {
        Value[] vs = new Value[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = sessionContext.getValueFactory().createValue(values[i], propertyType);
        }
        return toCoreValues(vs, sessionContext);
    }

    public static List<CoreValue> toCoreValues(Value[] values, SessionContext sessionContext) {
        List<CoreValue> cvs = new ArrayList<CoreValue>(values.length);
        for (Value jcrValue : values) {
            if (jcrValue != null) {
                cvs.add(toCoreValue(jcrValue, sessionContext));
            }
        }
        return cvs;
    }

    public static Value toValue(CoreValue coreValue, SessionContext sessionContext) {
        return sessionContext.getValueFactory().createValue(coreValue);
    }

    public static Value[] toValues(Iterable<CoreValue> coreValues, SessionContext sessionContext) {
        List<Value> values = new ArrayList<Value>();
        for (CoreValue cv : coreValues) {
            values.add(toValue(cv, sessionContext));
        }
        return values.toArray(new Value[values.size()]);
    }
}
