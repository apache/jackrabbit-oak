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
package org.apache.jackrabbit.oak.spi.security.authentication.token;

import java.util.Map;
import javax.annotation.CheckForNull;
import javax.jcr.Credentials;

/**
 * TokenProvider... TODO
 */
public interface TokenProvider {

    /**
     * Optional configuration parameter to set the token expiration time in ms.
     * Implementations that do not support this option will ignore any config
     * options with that name.
     */
    String PARAM_TOKEN_EXPIRATION = "tokenExpiration";

    /**
     * Optional configuration parameter to define the length of the key.
     * Implementations that do not support this option will ignore any config
     * options with that name.
     */
    String PARAM_TOKEN_LENGTH = "tokenLength";

    boolean doCreateToken(Credentials credentials);

    @CheckForNull
    TokenInfo createToken(Credentials credentials);

    @CheckForNull
    TokenInfo createToken(String userId, Map<String,?> attributes);

    @CheckForNull
    TokenInfo getTokenInfo(String token);

    boolean removeToken(TokenInfo tokenInfo);

    boolean resetTokenExpiration(TokenInfo tokenInfo, long loginTime);
}
