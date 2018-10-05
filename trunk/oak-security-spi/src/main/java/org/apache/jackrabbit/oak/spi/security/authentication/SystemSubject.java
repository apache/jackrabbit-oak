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

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;

/**
 * Internal utility providing access to a system internal subject instance.
 */
public final class SystemSubject {

    public static final Subject INSTANCE = createSystemSubject();

    /**
     * Private constructor
     */
    private SystemSubject() {}

    private static Subject createSystemSubject() {
        Set<? extends Principal> principals = Collections.singleton(SystemPrincipal.INSTANCE);
        return new Subject(true, principals, Collections.<Object>emptySet(), Collections.<Object>emptySet());
    }
}