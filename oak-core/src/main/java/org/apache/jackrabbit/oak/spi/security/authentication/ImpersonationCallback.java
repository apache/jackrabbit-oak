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
import javax.security.auth.callback.Callback;

/**
 * Callback for a {@link javax.security.auth.callback.CallbackHandler} to ask
 * for a the impersonating {@link javax.security.auth.Subject} to create a
 * {@link javax.jcr.Session} to access the {@link javax.jcr.Repository}.
 */
public class ImpersonationCallback implements Callback {

    /**
     * The impersonating {@link javax.security.auth.Subject}.
     */
    private Subject impersonatingSubject;

    /**
     * Sets the impersonator in this callback.
     *
     * @param impersonatingSubject The impersonator to set on this callback.
     */
    public void setImpersonator(Subject impersonatingSubject) {
        this.impersonatingSubject = impersonatingSubject;
    }

    /**
     * Returns the impersonator {@link Subject} set on this callback or
     * <code>null</code> if not set.
     *
     * @return the impersonator {@link Subject} set on this callback or
     * <code>null</code> if not set.
     */
    public Subject getImpersonator() {
        return impersonatingSubject;
    }
}