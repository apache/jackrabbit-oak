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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.TreeUtil;

/**
 * UserContext... TODO
 */
final class UserContext implements Context {

    private static final Context INSTANCE = new UserContext();

    private UserContext() {
    }

    static Context getInstance() {
        return INSTANCE;
    }

    //------------------------------------------------------------< Context >---
    @Override
    public boolean definesProperty(Tree parent, PropertyState property) {
        String ntName = TreeUtil.getPrimaryTypeName(parent);
        if (UserConstants.NT_REP_USER.equals(ntName)) {
            return UserConstants.USER_PROPERTY_NAMES.contains(property.getName());
        } else if (UserConstants.NT_REP_GROUP.equals(ntName)) {
            return UserConstants.GROUP_PROPERTY_NAMES.contains(property.getName());
        } else if (UserConstants.NT_REP_MEMBERS.equals(ntName)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean definesTree(Tree tree) {
        String ntName = TreeUtil.getPrimaryTypeName(tree);
        return UserConstants.NT_REP_GROUP.equals(ntName) || UserConstants.NT_REP_USER.equals(ntName) || UserConstants.NT_REP_MEMBERS.equals(ntName);
    }
}