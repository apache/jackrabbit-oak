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
package org.apache.jackrabbit.oak.spi.security.authentication.callback;

import javax.security.auth.callback.Callback;

import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;

/**
 * Callback implementation used to pass a {@link PrincipalProvider} to the
 * login module.
 */
public class PrincipalProviderCallback implements Callback {

    private PrincipalProvider principalProvider;

    /**
     * Returns the principal provider as set using
     * {@link #setPrincipalProvider(org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider)}
     * or {@code null}.
     *
     * @return an instance of {@code PrincipalProvider} or {@code null} if no
     * provider has been set before.
     */
    public PrincipalProvider getPrincipalProvider() {
        return principalProvider;
    }

    /**
     * Sets the {@code PrincipalProvider} that is being used during the
     * authentication process.
     *
     * @param principalProvider The principal provider to use during the
     * authentication process.
     */
    public void setPrincipalProvider(PrincipalProvider principalProvider) {
        this.principalProvider = principalProvider;
    }
}