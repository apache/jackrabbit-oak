/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.run.osgi

import org.apache.felix.scr.Component
import org.apache.felix.scr.ScrService
import org.apache.jackrabbit.JcrConstants
import org.apache.jackrabbit.commons.JcrUtils
import org.apache.jackrabbit.oak.plugins.index.IndexConstants
import org.junit.Before
import org.junit.Ignore
import org.junit.Test

import javax.jcr.Node
import javax.jcr.Session

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE

@Ignore("OAK-3366")
class PropertyIndexReindexingTest extends AbstractRepositoryFactoryTest{

    @Before
    void setupRepo() {
        config[REPOSITORY_CONFIG_FILE] = createConfigValue("oak-base-config.json", "oak-tar-config.json")
    }

    @Test
    public void propertyIndexState() throws Exception{
        repository = repositoryFactory.getRepository(config)

        //1. Save a node with 'foo' property
        Session s = createAdminSession()
        s.getRootNode().addNode("a1").setProperty("foo", "bar")
        s.save();  s.logout();

        //2. Create a property index for 'foo'
        s = createAdminSession()
        Node fooIndex = JcrUtils.getOrCreateByPath("/oak:index/foo", JcrConstants.NT_UNSTRUCTURED,
                "oak:QueryIndexDefinition", s, false);
        fooIndex.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "property");
        fooIndex.setProperty(IndexConstants.PROPERTY_NAMES, ["foo"] as String[]);
        s.save();s.logout();

        //3. By the last save reindex flag should be false
        s = createAdminSession()
        assert false == s.getProperty("/oak:index/foo/reindex").boolean
        s.logout()

        //4. Disable the PropertyIndexEditor
        ScrService scr = getService(ScrService.class)
        Component[] c = scr.getComponents('org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider')
        assert c

        c[0].disable()

        //5. Save another node with 'foo' property
        s = createAdminSession()
        s.getRootNode().addNode("a2").setProperty("foo", "bar")
        s.save();  s.logout();

        //Either last save should not have succeeded or at least reindex flag is not set to
        //true
        s = createAdminSession()
        assert false == s.getProperty("/oak:index/foo/reindex").boolean
        s.logout()
    }
}
