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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;

/**
 * Principal used to mark a principal that results from a login using
 * {@link javax.jcr.GuestCredentials} or any other credentials that result in
 * "anonymous" login. Whether or not this principal is used as single or as
 * additional non-group principal is left to the implementation.
 */
public final class AnonymousPrincipal implements Principal {

    public static final String NAME = "anonymous";

    public static final AnonymousPrincipal INSTANCE = new AnonymousPrincipal();

    private AnonymousPrincipal() { }

    //----------------------------------------------------------< Principal >---
    @Override
    public String getName() {
        return NAME;
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public String toString() {
        return NAME + " principal";
    }
}
