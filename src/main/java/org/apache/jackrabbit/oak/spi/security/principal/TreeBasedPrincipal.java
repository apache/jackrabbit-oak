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

import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.PathMapper;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;

import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * TreeBasedPrincipal...
 */
public class TreeBasedPrincipal extends PrincipalImpl implements ItemBasedPrincipal {

    private final String path;
    private final PathMapper pathMapper;

    public TreeBasedPrincipal(Tree tree, PathMapper pathMapper) {
        super(getPrincipalName(tree));
        this.pathMapper = pathMapper;
        this.path = tree.getPath();
    }

    public TreeBasedPrincipal(String principalName, Tree tree, PathMapper pathMapper) {
        this(principalName, tree.getPath(), pathMapper);
    }

    public TreeBasedPrincipal(String principalName, String oakPath, PathMapper pathMapper) {
        super(principalName);
        this.pathMapper = pathMapper;
        this.path = oakPath;
    }

    public String getOakPath() {
        return path;
    }

    //-------------------------------------------------< ItemBasedPrincipal >---
    @Override
    public String getPath() {
        return pathMapper.getJcrPath(path);
    }

    //--------------------------------------------------------------------------
    private static String getPrincipalName(Tree tree) {
        PropertyState prop = tree.getProperty(UserConstants.REP_PRINCIPAL_NAME);
        if (prop == null) {
            throw new IllegalArgumentException("Tree doesn't have rep:principalName property");
        }
        return prop.getValue(STRING);
    }
}