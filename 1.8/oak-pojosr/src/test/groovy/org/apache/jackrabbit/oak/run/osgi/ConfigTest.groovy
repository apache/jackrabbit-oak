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

import org.apache.felix.connect.launch.BundleDescriptor
import org.apache.felix.connect.launch.PojoServiceRegistry
import groovy.json.JsonOutput
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.osgi.framework.Constants
import org.osgi.service.cm.Configuration
import org.osgi.service.cm.ConfigurationAdmin

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.*

class ConfigTest {
    TestRepositoryFactory factory = new TestRepositoryFactory()
    Map config
    File workDir
    PojoServiceRegistry registry
    ConfigurationAdmin cm

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"))

    @Before
    void setUp(){
        workDir = tmpFolder.getRoot();
        config  = [
                (REPOSITORY_HOME) : workDir.absolutePath,
                'magic.spell' : 'Alohomora'
        ]
    }

    @After
    void shutDown(){
        OakOSGiRepositoryFactory.shutdown(registry, 5)
    }

    @Test
    void testRuntimeConfig(){
        config[REPOSITORY_CONFIG] = createConfigMap()

        initRegistry(config)

        assertConfig()
    }

    @Test
    void testFileConfig(){
        def jf1 = new File(workDir, "config1.json")
        def jf2 = new File(workDir, "config2.json")
        jf1 << JsonOutput.toJson(createConfigMap())
        jf2 << JsonOutput.toJson([bar : [a:'a3', b:4]])
        config[REPOSITORY_CONFIG_FILE] = "${jf1.absolutePath},${jf2.absolutePath}" as String

        initRegistry(config)

        assertConfig()

        Configuration c1 = cm.getConfiguration('bar', null)
        assert c1.properties
        assert c1.properties.get('a') == 'a3'
        assert c1.properties.get('b') == 4
    }

    @Test
    void testConfigSync(){
        config[REPOSITORY_CONFIG] = [
                foo : [a:'a', b:1],
                bar : [a:'a1', b:2],
                foo2 : [a:'a2', b:2]
        ]
        initRegistry(config)
        Configuration c = cm.getConfiguration('baz')
        c.update(new Hashtable([a :'a2']))

        assert cm.getConfiguration('baz').properties.get('a') == 'a2'
        assert cm.getConfiguration('foo').properties.get('a') == 'a'
        assert cm.getConfiguration('bar').properties.get('a') == 'a1'
        assert cm.getConfiguration('foo2').properties.get('a') == 'a2'

        shutDown()

        //Now re init and remove the pid bar
        config[REPOSITORY_CONFIG] = [
                foo : [a:'a-new', b:1],
                foo2 : [a:'a2', b:2]
        ]
        initRegistry(config)

        assert cm.getConfiguration('baz').properties.get('a') == 'a2'
        assert cm.getConfiguration('foo').properties.get('a') == 'a-new'
        assert cm.getConfiguration('bar').properties == null
        assert cm.getConfiguration('foo2').properties.get('a') == 'a2'
    }

    private static Map createConfigMap() {
        [
                'foo'            : [a: 'a', b: 1, c:'${magic.spell}'],
                'foo.bar-default': [a: 'a1', b: 2],
                'foo.bar-simple' : [a: 'a2', b: 3],
        ]
    }

    private void assertConfig() {
        Configuration c1 = cm.getConfiguration('foo', null)
        assert c1.properties
        assert c1.properties.get('a') == 'a'
        assert c1.properties.get('b') == 1
        assert c1.properties.get('c') == 'Alohomora'

        Configuration[] fcs = cm.listConfigurations('(service.factoryPid=foo.bar)')
        assert fcs.size() == 2
    }

    private void initRegistry(Map config){
        registry = factory.initializeServiceRegistry(config)
        cm = registry.getService(registry.getServiceReference(ConfigurationAdmin.class.name)) as ConfigurationAdmin
    }

    private static class TestRepositoryFactory extends OakOSGiRepositoryFactory {
        @Override
        protected List<BundleDescriptor> processDescriptors(List<BundleDescriptor> descriptors) {
            //skip the oak bundles to prevent repository initialization
            return super.processDescriptors(descriptors).findAll {BundleDescriptor bd ->
                !bd.headers[Constants.BUNDLE_SYMBOLICNAME]?.startsWith('org.apache.jackrabbit')
            }
        }
    }
}
