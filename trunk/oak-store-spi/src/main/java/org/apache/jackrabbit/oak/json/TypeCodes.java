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
package org.apache.jackrabbit.oak.json;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.jcr.PropertyType;

/**
 * TypeCodes maps between {@code Type} and the code used to prefix
 * its json serialisation.
 */
public final class TypeCodes {

    public static final String EMPTY_ARRAY = "[0]:";

    private static final Map<Integer, String> TYPE2CODE = new HashMap<Integer, String>();
    private static final Map<String, Integer> CODE2TYPE = new HashMap<String, Integer>();

    static {
        for (int type = PropertyType.UNDEFINED; type <= PropertyType.DECIMAL; type++) {
            String code = type == PropertyType.BINARY
                    ? ":blobId"  // See class comment for MicroKernel and OAK-428
                    : PropertyType.nameFromValue(type).substring(0, 3).toLowerCase(Locale.ENGLISH);
            TYPE2CODE.put(type, code);
            CODE2TYPE.put(code, type);
        }
    }

    private TypeCodes() { }

    /**
     * Encodes the given {@code propertyName} of the given {@code propertyType} into
     * a json string, which is prefixed with a type code.
     * @param propertyType  type of the property
     * @param propertyName  name of the property
     * @return  type code prefixed json string
     */
    public static String encode(int propertyType, String propertyName) {
        String typeCode = checkNotNull(TYPE2CODE.get(propertyType));
        return typeCode + ':' + propertyName;
    }

    /**
     * Splits a {@code jsonString}, which is prefixed with a type code
     * at the location where the prefix ends.
     * @param jsonString  json string to split
     * @return  the location where the prefix ends or -1 if no prefix is present
     */
    public static int split(String jsonString) {
        if (jsonString.startsWith(":blobId:")) {  // See OAK-428
            return 7;
        }
        else if (jsonString.length() >= 4 && jsonString.charAt(3) == ':') {
            return 3;
        }
        else {
            return -1;
        }
    }

    /**
     * Decode the type encoded into {@code jsonString} given its split.
     * @param split  split of the json string
     * @param jsonString  json string
     * @return  decoded type. {@code PropertyType.UNDEFINED} if none or split is not within {@code jsonString}.
     */
    public static int decodeType(int split, String jsonString) {
        if (split == -1 || split > jsonString.length()) {
            return PropertyType.UNDEFINED;
        }
        else {
            Integer type = CODE2TYPE.get(jsonString.substring(0, split));
            return type == null
                    ? PropertyType.UNDEFINED
                    : type;
        }
    }

    /**
     * Decode the property name encoded into a {@code jsonString} given its split.
     * @param split  split of the json string
     * @param jsonString  json string
     * @return  decoded property name. Or {@code jsonString} if split is not with {@code jsonString}.
     */
    public static String decodeName(int split, String jsonString) {
        if (split == -1 || split >= jsonString.length()) {
            return jsonString;
        }
        else {
            return jsonString.substring(split + 1);
        }
    }

}