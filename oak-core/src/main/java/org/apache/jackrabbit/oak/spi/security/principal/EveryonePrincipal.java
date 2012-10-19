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
import java.util.Enumeration;

import org.apache.jackrabbit.api.security.principal.JackrabbitPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in principal group that has every other principal as member.
 */
public class EveryonePrincipal implements JackrabbitPrincipal, java.security.acl.Group {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(EveryonePrincipal.class);

    public static final String NAME = "everyone";

    private static final EveryonePrincipal INSTANCE = new EveryonePrincipal();

    private EveryonePrincipal() { }

    public static EveryonePrincipal getInstance() {
        return INSTANCE;
    }

    //----------------------------------------------------------< Principal >---
    @Override
    public String getName() {
        return NAME;
    }

    //--------------------------------------------------------------< Group >---
    @Override
    public boolean addMember(Principal user) {
        return false;
    }

    @Override
    public boolean removeMember(Principal user) {
        throw new UnsupportedOperationException("Cannot remove a member from the everyone group.");
    }

    @Override
    public boolean isMember(Principal member) {
        return !member.equals(this);
    }

    @Override
    public Enumeration<? extends Principal> members() {
        throw new UnsupportedOperationException("Not implemented.");
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Principal) {
            return NAME.equals(((Principal) obj).getName());
        }
        return false;
    }

    @Override
    public String toString() {
        return NAME + " principal";
    }
}