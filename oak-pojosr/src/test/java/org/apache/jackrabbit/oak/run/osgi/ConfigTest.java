/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.run.osgi;

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_HOME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.felix.connect.launch.BundleDescriptor;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;


public class ConfigTest {

    private TestRepositoryFactory factory = new TestRepositoryFactory();
    private Map<String, Object> config;
    private File workDir;
    private PojoServiceRegistry registry;
    private ConfigurationAdmin cm;

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"));

    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        workDir = tmpFolder.getRoot();
        config = new HashMap<>(Map.of(
                REPOSITORY_HOME, workDir.getAbsolutePath(),
                "magic.spell", "Alohomora"));
        objectMapper = new ObjectMapper();
    }

    @After
    public void shutDown() throws BundleException {
        OakOSGiRepositoryFactory.shutdown(registry, 5);
    }

    @Test
    public void testRuntimeConfig() throws Exception {
        config.put(REPOSITORY_CONFIG, createConfigMap());
        initRegistry(config);
        assertConfig();
    }

    @Test
    public void testFileConfig() throws Exception {
        // Open 2 files and write a string
        File jf1 = new File(workDir, "config1.json");
        File jf2 = new File(workDir, "config2.json");

        objectMapper.writeValue(jf1, createConfigMap());
        objectMapper.writeValue(jf2, Map.of("bar", Map.of("a", "a3", "b", 4)));
        config.put(REPOSITORY_CONFIG_FILE, String.format("%s,%s", jf1.getAbsolutePath(), jf2.getAbsolutePath()));

        initRegistry(config);
        assertConfig();

        Configuration c1 = cm.getConfiguration("bar", null);
        assertNotNull(c1.getProperties());
        assertEquals("a3", c1.getProperties().get("a"));
        assertEquals("4", c1.getProperties().get("b").toString());
    }

    @Test
    public void testConfigSync() throws Exception {
        config.put(REPOSITORY_CONFIG, Map.of(
                "foo", Map.of("a", "a", "b", 1),
                "bar", Map.of("a", "a1", "b", 2),
                "foo2", Map.of("a", "a2", "b", 2)
        ));
        initRegistry(config);
        Configuration c = cm.getConfiguration("baz");
        c.update(new Hashtable<>(Map.of("a", "a2")));

        assertEquals("a2", cm.getConfiguration("baz").getProperties().get("a"));
        assertEquals("a", cm.getConfiguration("foo").getProperties().get("a"));
        assertEquals("a1", cm.getConfiguration("bar").getProperties().get("a"));
        assertEquals("a2", cm.getConfiguration("foo2").getProperties().get("a"));

        shutDown();

        config.put(REPOSITORY_CONFIG, Map.of(
                "foo", Map.of("a", "a-new", "b", 1),
                "foo2", Map.of("a", "a2", "b", 2)
        ));
        initRegistry(config);

        assertEquals("a2",cm.getConfiguration("baz").getProperties().get("a"));
        assertEquals("a-new",cm.getConfiguration("foo").getProperties().get("a"));
        assertNull(cm.getConfiguration("bar").getProperties());
        assertEquals("a2",cm.getConfiguration("foo2").getProperties().get("a"));
    }

    private static Map<String, Map<String, Object>> createConfigMap() {
        return Map.of(
                "foo", Map.of("a", "a", "b", 1, "c", "${magic.spell}"),
                "foo.bar-default", Map.of("a", "a1", "b", 2),
                "foo.bar-simple", Map.of("a", "a2", "b", 3)
        );
    }

    private void assertConfig() throws Exception {
        Configuration c1 = cm.getConfiguration("foo", null);
        assertNotNull(c1.getProperties());
        assertEquals("a", c1.getProperties().get("a"));
        // It returns different number types (long or integer) depending how its read
        assertEquals("1", c1.getProperties().get("b").toString());
        assertEquals("Alohomora", c1.getProperties().get("c"));

        Configuration[] fcs = cm.listConfigurations("(service.factoryPid=foo.bar)");
        assertEquals(2, fcs.length);
    }

    private void initRegistry(Map<String, Object> config) {
        registry = factory.initializeServiceRegistry(config);
        cm = (ConfigurationAdmin) registry.getService(registry.getServiceReference(ConfigurationAdmin.class.getName()));
    }

    private static class TestRepositoryFactory extends OakOSGiRepositoryFactory {

        @Override
        protected List<BundleDescriptor> processDescriptors(List<BundleDescriptor> descriptors) {
            return super.processDescriptors(descriptors).stream()
                    .filter(bd -> !bd.getHeaders()
                            .get(Constants.BUNDLE_SYMBOLICNAME)
                            .startsWith("org.apache.jackrabbit"))
                    .collect(Collectors.toList());
        }
    }
}
