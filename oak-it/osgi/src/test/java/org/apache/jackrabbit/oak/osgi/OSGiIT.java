/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.osgi;

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.osgi.framework.BundleContext;

@RunWith(JUnit4TestRunner.class)
public class OSGiIT {

    private final File TARGET = new File("target");

    @Configuration
    public Option[] configuration() throws IOException, URISyntaxException {
        File base = new File(TARGET, "test-bundles");
        return CoreOptions.options(
                junitBundles(),
                bundle(new File(base, "oak-mk.jar").toURI().toURL().toString()),
                bundle(new File(base, "oak-core.jar").toURI().toURL().toString()),
                bundle(new File(base, "oak-jcr.jar").toURI().toURL().toString()));
    }
 
    @Test
    public void testMicroKernel(BundleContext bc) throws Exception {
    }

    @Test
    public void testOakRepository(BundleContext bc) throws Exception {
    }

    @Test
    public void testJcrRepository(BundleContext bc) throws Exception {
    }

}
