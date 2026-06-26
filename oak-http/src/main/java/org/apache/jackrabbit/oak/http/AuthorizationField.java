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
package org.apache.jackrabbit.oak.http;

import com.fasterxml.jackson.databind.ser.Serializers;
import org.apache.jackrabbit.util.Base64;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import java.util.Enumeration;
import java.util.Locale;
import java.util.NoSuchElementException;

public class AuthorizationField {

    private AuthorizationField() {
        // no constructor
    }

    public static SimpleCredentials valueOf(Enumeration<String> values) throws LoginException {
        String field ;
        try {
            field = values.nextElement();
        } catch (NoSuchElementException ex) {
            throw new LoginException(ex.getMessage());
        }

        if (values.hasMoreElements()) {
            throw new LoginException("Authorization field has multiple field line values");
        }

        return parseCredentials(field);
    }

    private static SimpleCredentials parseCredentials(String rawFieldValue) throws LoginException {
        boolean hasControls = rawFieldValue.chars().anyMatch(c -> c < ' ');
        if (hasControls) {
            throw new LoginException("Control characters are not allowed");
        }

        String fieldValue = rawFieldValue.trim();

        if (fieldValue.toLowerCase(Locale.ENGLISH).startsWith("basic ")) {
            String token68 = fieldValue.substring("basic ".length());
            String decoded  = Base64.decode(token68);
            int colon = decoded.indexOf(':');
            if (colon < 0) {
                throw new LoginException(
                        "Malformed Basic credentials: missing ':' separator");
            }
            String userId = decoded.substring(0, colon);
            String password = decoded.substring(colon + 1);

            return new SimpleCredentials(userId, password.toCharArray());
        } else {
            throw new LoginException("Only Basic Authentication supported");
        }
    }
}
