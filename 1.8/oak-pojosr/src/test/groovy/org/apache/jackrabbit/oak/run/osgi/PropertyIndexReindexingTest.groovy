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

import org.apache.felix.connect.launch.PojoServiceRegistry
import org.apache.jackrabbit.JcrConstants
import org.apache.jackrabbit.commons.JcrUtils
import org.apache.jackrabbit.oak.plugins.index.IndexConstants
import org.junit.Before
import org.junit.Test

import javax.jcr.Node
import javax.jcr.Repository
import javax.jcr.Session
import java.util.concurrent.TimeUnit

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE

class PropertyIndexReindexingTest extends AbstractRepositoryFactoryTest{

    @Before
    void setupRepo() {
        config[REPOSITORY_CONFIG_FILE] = createConfigValue("oak-base-config.json", "oak-tar-config.json")
    }

    @Test
    public void propertyIndexState() throws Exception{
        repository = repositoryFactory.getRepository(config)
        PojoServiceRegistry registry = getRegistry()

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
        assert !s.getProperty("/oak:index/foo/reindex").boolean
        s.logout()

        //4. Disable the PropertyIndexEditor
        def indexComponent = 'org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider'
        disableComponent(indexComponent)
        TimeUnit.SECONDS.sleep(1)
       assert registry.getServiceReference(Repository.class.name) == null : "Repository should be unregistered " +
               "if no property index editor found"

        //5. Re-enable the editor and wait untill repository gets re-registered
        enableComponent(indexComponent)
        getServiceWithWait(Repository.class, registry.bundleContext)

        //6. Reindex flag should be stable
        s = createAdminSession()
        assert !s.getProperty("/oak:index/foo/reindex").boolean
        s.logout()
    }
}
