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

import org.apache.jackrabbit.api.security.principal.JackrabbitPrincipal;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of the {@code JackrabbitPrincipal} interface.
 */
public class PrincipalImpl implements JackrabbitPrincipal {

    private final String name;

    public PrincipalImpl(String name) {
        this.name = checkNotNull(name);
    }

    //----------------------------------------------------------< Principal >---
    @Override
    public String getName() {
        return name;
    }

    //-------------------------------------------------------------< Object >---
    /**
     * Two principals are equal, if their names are.
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof JackrabbitPrincipal) {
            return name.equals(((JackrabbitPrincipal) obj).getName());
        }
        return false;
    }

    /**
     * @return the hash code of the principals name.
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName()).append(':').append(name);
        return sb.toString();
    }
}