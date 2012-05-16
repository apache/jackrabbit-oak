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
package org.apache.jackrabbit.oak.spi.security.authentication;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Credentials;

/**
 * ImpersonationCredentials...  TODO
 */
public class ImpersonationCredentials implements Credentials {

    private final Credentials baseCredentials;
    private final AuthInfo authInfo;

    public ImpersonationCredentials(Credentials baseCredentials, AuthInfo authInfo) {
        this.baseCredentials = baseCredentials;
        this.authInfo = authInfo;
    }

    public Credentials getBaseCredentials() {
        return baseCredentials;
    }

    public AuthInfo getImpersonatorInfo() {
        return authInfo;
    }
}