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

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.spi.LoginModule;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;

/**
 * LoginModule implementation for {@code UserIDTest}
 */
public class UserIDTestLoginModule implements LoginModule {

    private Subject subject;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> stringMap, Map<String, ?> stringMap2) {
        this.subject = subject;
    }

    @Override
    public boolean login() {
        return true;
    }

    @Override
    public boolean commit() {
        if (!subject.isReadOnly()) {
            // be defensive: remove all potentially added "AuthInfo' objects.
            Set<AuthInfo> ais = subject.getPublicCredentials(AuthInfo.class);
            if (!ais.isEmpty()) {
                subject.getPublicCredentials().removeAll(ais);
            }
            // and finally add the one that produces the desired result:
            String userID = null;
            subject.getPublicCredentials().add(new AuthInfoImpl(userID, Collections.<String, Object>emptyMap(), Collections.<Principal>emptySet()));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean abort() {
        return true;
    }

    @Override
    public boolean logout() {
        return true;
    }
}