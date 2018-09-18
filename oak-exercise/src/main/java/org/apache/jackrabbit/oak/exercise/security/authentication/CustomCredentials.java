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
package org.apache.jackrabbit.oak.exercise.security.authentication;

import java.util.Map;
import javax.jcr.Credentials;

import com.google.common.collect.ImmutableMap;

class CustomCredentials implements Credentials {

    private final String loginID;
    private final String password;
    private final Map<String, String> attributes;

    CustomCredentials(String loginID, String password, Map<String,String> attributes) {
        this.loginID = loginID;
        this.password = password;
        this.attributes = ImmutableMap.copyOf(attributes);
    }

    String getLoginID() {
        return loginID;
    }

    String getPassword() {
        return password;
    }

    Map<String, String> getAttributes() {
        return attributes;
    }
}