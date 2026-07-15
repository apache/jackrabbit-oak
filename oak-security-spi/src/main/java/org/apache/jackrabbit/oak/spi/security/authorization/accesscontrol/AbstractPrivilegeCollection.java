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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeUtil;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import java.util.Objects;

abstract class AbstractPrivilegeCollection implements PrivilegeCollection {
    
    private final PrivilegeBits privilegeBits;

    AbstractPrivilegeCollection(@NotNull PrivilegeBits privilegeBits) {
        this.privilegeBits = privilegeBits;
    }
    
    abstract @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider();
    abstract @NotNull NamePathMapper getNamePathMapper();

    @Override
    public boolean includes(@NotNull String... privilegeNames) throws RepositoryException {
        if (privilegeNames.length == 0) {
            return true;
        }
        if (privilegeBits.isEmpty()) {
            return false;
        }
        PrivilegeBits toTest = getPrivilegeBitsProvider().getBits(PrivilegeUtil.getOakNames(privilegeNames, getNamePathMapper()), true);
        return privilegeBits.includes(toTest);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(privilegeBits);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof AbstractPrivilegeCollection) {
            AbstractPrivilegeCollection other = (AbstractPrivilegeCollection) obj;
            return privilegeBits.equals(other.privilegeBits);
        }
        return false;
    }
}