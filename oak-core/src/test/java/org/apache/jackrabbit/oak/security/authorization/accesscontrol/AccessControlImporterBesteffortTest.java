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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import javax.jcr.PropertyType;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AccessControlImporterBesteffortTest extends AccessControlImporterBaseTest{

    @Override
    String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    @Test
    public void testStartAceChildInfoUnknownPrincipal() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of(unknownPrincipalInfo));
    }

    @Test
    public void testImportWithUnknownPrincipal() throws Exception {
        init();
        importer.start(aclTree);

        PropInfo privs = new PropInfo(REP_PRIVILEGES, PropertyType.NAME, createTextValues(PrivilegeConstants.JCR_READ));
        importer.startChildInfo(aceInfo, ImmutableList.of(unknownPrincipalInfo, privs));
        importer.endChildInfo();

        importer.end(aclTree);

        Tree aceTree = aclTree.getChildren().iterator().next();
        assertEquals(unknownPrincipalInfo.getValue(PropertyType.STRING).getString(), TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
    }
}