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
package org.apache.jackrabbit.oak.security.authentication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

/**
 * ImpersonationCredentials...
 */
public class ImpersonationCredentials {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ImpersonationCredentials.class);

     /**
      * Constant for backwards compatibility with jackrabbit 2.x.
      * It defines the name of the {@code SimpleCredentials} attribute where
      * the {@code Subject} of the <i>impersonating</i> {@code Session}
      * is stored.
      *
      * @see javax.jcr.Session#impersonate(javax.jcr.Credentials)
      */
    public static final String IMPERSONATOR_ATTRIBUTE = "org.apache.jackrabbit.core.security.impersonator";

    private final String userID;
    private Subject impersonatingSubject;

    public ImpersonationCredentials(String userID) {
        this.userID = userID;
    }

    public String getUserID() {
        return userID;
    }

    public Subject getImpersonatingSubject() {
        return impersonatingSubject;
    }

    public void setImpersonatingSubject(Subject impersonatingSubject) {
        this.impersonatingSubject = impersonatingSubject;
    }
}