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
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

/**
 * Bridge class that connects the JAAS {@link javax.security.auth.login.LoginContext} class with the
 * {@link LoginContext} interface used by Oak.
 */
public class JaasLoginContext extends javax.security.auth.login.LoginContext implements LoginContext {

    public JaasLoginContext(String name) throws LoginException {
        super(name);
    }

    public JaasLoginContext(String name, Subject subject) throws LoginException {
        super(name, subject);
    }

    public JaasLoginContext(String name, CallbackHandler handler) throws LoginException {
        super(name, handler);
    }

    public JaasLoginContext(String name, Subject subject, CallbackHandler handler)
            throws LoginException {
        super(name, subject, handler);
    }

    public JaasLoginContext(String name, Subject subject, CallbackHandler handler,
                            Configuration configuration) throws LoginException {
        super(name, subject, handler, configuration);
    }

}
