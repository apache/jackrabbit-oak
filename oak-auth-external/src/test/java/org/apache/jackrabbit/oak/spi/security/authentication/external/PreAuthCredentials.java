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

package org.apache.jackrabbit.oak.spi.security.authentication.external;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;

final class PreAuthCredentials implements Credentials {

    static final String PRE_AUTH_DONE = "pre_auth_done";
    static final String PRE_AUTH_FAIL = "pre_auth_fail";

    private final String userId;
    private String msg;

    PreAuthCredentials(@Nullable String userId) {
        this.userId = userId;
    }

    @CheckForNull
    String getUserId() {
        return userId;
    }

    @CheckForNull
    String getMessage() {
        return msg;
    }

    void setMessage(@Nonnull String message) {
        msg = message;
    }
}
