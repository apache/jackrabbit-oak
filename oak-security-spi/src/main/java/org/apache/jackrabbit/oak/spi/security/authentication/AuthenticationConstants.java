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

/**
 * Constants used by the authentication framework.
 */
public interface AuthenticationConstants {

    /**
     * Key of the sharedState entry referring to a valid login ID that is shared
     * between multiple login modules.
     */
    String SHARED_KEY_LOGIN_NAME = "javax.security.auth.login.name";

    /**
     * Key of the sharedState entry referring to validated Credentials that is
     * shared between multiple login modules.
     */
    String SHARED_KEY_CREDENTIALS = "org.apache.jackrabbit.credentials";

    /**
     * Key of the sharedState entry referring to public attributes that are shared
     * between multiple login modules.
     */
    String SHARED_KEY_ATTRIBUTES = "javax.security.auth.login.attributes";

    /**
     * Key of the sharedState entry referring to pre authenticated login information that is shared
     * between multiple login modules.
     */
    String SHARED_KEY_PRE_AUTH_LOGIN = PreAuthenticatedLogin.class.getName();

    /**
     * Name of the attribute storing the external identifier in Credentials
     */
    String SHARED_ATTRIBUTE_EXTERNAL_ID = ":externalId";

}