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
package org.apache.jackrabbit.oak.benchmark.authorization;

import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;

import java.util.ArrayList;
import java.util.List;

public class HasPermissionHasItemGetItemTest extends AbstractHasItemGetItemTest {

    private static final List<String> PERMISSIONS = new ArrayList<>(Permissions.PERMISSION_NAMES.values());

    public HasPermissionHasItemGetItemTest(int itemsToRead, int numberOfACEs, int numberOfGroups, boolean doReport) {
        super(itemsToRead, numberOfACEs, numberOfGroups, doReport);
    }

    @NotNull
    @Override
    String additionalMethodName() {
        return "hasPermission";
    }

    @Override
    void additionalOperations(@NotNull String path, @NotNull Session s, @NotNull AccessControlManager acMgr) {
        try {
            String actions = Text.implode((String[]) Utils.getRandom(PERMISSIONS, 3).toArray(new String[0]), ",");
            s.hasPermission(path, actions);
        } catch (RepositoryException e) {
            if (doReport) {
                e.printStackTrace(System.out);
            }
        }
    }

    @NotNull
    @Override
    protected String getTestNodeName() {
        return "HasPermissionHasItemGetItemTest";
    }
}