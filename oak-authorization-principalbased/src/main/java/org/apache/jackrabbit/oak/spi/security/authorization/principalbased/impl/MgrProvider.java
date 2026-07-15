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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.jetbrains.annotations.NotNull;

interface MgrProvider {

    @NotNull
    SecurityProvider getSecurityProvider();

    void reset(@NotNull Root root, NamePathMapper namePathMapper);

    @NotNull
    Root getRoot();

    @NotNull
    NamePathMapper getNamePathMapper();

    @NotNull
    Context getContext();

    @NotNull
    PrivilegeManager getPrivilegeManager();

    @NotNull
    PrivilegeBitsProvider getPrivilegeBitsProvider();

    @NotNull
    PrincipalManager getPrincipalManager();

    @NotNull
    RestrictionProvider getRestrictionProvider();

    @NotNull
    TreeProvider getTreeProvider();

    @NotNull
    RootProvider getRootProvider();
}