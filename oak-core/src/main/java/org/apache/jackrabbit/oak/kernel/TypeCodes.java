/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.kernel;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.jcr.PropertyType;

/**
 * TypeCodes maps between {@code Type} and the code used to prefix
 * its json serialisation.
 */
public class TypeCodes {
    private static final Map<Integer, String> TYPE2CODE = new HashMap<Integer, String>();
    private static final Map<String, Integer> CODE2TYPE = new HashMap<String, Integer>();

    static {
        for (int type = PropertyType.UNDEFINED; type <= PropertyType.DECIMAL; type++) {
            String code = PropertyType.nameFromValue(type).substring(0, 3).toLowerCase(Locale.ENGLISH);
            TYPE2CODE.put(type, code);
            CODE2TYPE.put(code, type);
        }
    }

    private TypeCodes() { }

    /**
     * Returns {@code true} if the specified JSON String represents a value
     * serialization that is prefixed with a type code.
     *
     * @param jsonString The JSON String representation of the value of a {@code PropertyState}
     * @return {@code true} if the {@code jsonString} starts with a type
     * code; {@code false} otherwise.
     */
    public static boolean startsWithCode(String jsonString) {
        return jsonString.length() >= 4 && jsonString.charAt(3) == ':';
    }

    /**
     * Get the type code for the given property type.
     *
     * @param propertyType the property type
     * @return the type code
     */
    public static String getCodeForType(int propertyType) {
        return TYPE2CODE.get(propertyType);
    }

    /**
     * Get the property type for the given type code.
     * @param code  the type code
     * @return  the property type.
     */
    public static int getTypeForCode(String code) {
        return CODE2TYPE.get(code);
    }

}