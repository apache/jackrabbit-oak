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

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

/**
 * Interface version of the JAAS {@link javax.security.auth.login.LoginContext}
 * class. It is used to make integration of non-JAAS authentication components
 * easier while still retaining full JAAS support. The {@link JaasLoginContext}
 * class acts as a bridge that connects the JAAS
 * {@link javax.security.auth.login.LoginContext} class with this interface.
 */
public interface LoginContext {

    /**
     * @see javax.security.auth.login.LoginContext#getSubject()
     */
    Subject getSubject();

    /**
     * @see javax.security.auth.login.LoginContext#login()
     */
    void login() throws LoginException;

    /**
     * @see javax.security.auth.login.LoginContext#logout()
     */
    void logout() throws LoginException;

}
