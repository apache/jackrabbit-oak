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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.jcr.Repository;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.options.UrlProvisionOption;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;

@RunWith(JUnit4TestRunner.class)
public class OSGiIT {

    @Configuration
    public Option[] configuration() throws IOException, URISyntaxException {
        return CoreOptions.options(
                junitBundles(),
                mavenBundle("org.apache.felix", "org.apache.felix.scr", "1.6.0"),
                jarBundle("jcr.jar"),
                jarBundle("guava.jar"),
                jarBundle("jackrabbit-api.jar"),
                jarBundle("jackrabbit-jcr-commons.jar"),
                jarBundle("oak-commons.jar"),
                jarBundle("oak-mk-api.jar"),
                jarBundle("oak-mk.jar"),
                jarBundle("oak-mk-remote.jar"),
                jarBundle("oak-core.jar"),
                jarBundle("oak-lucene.jar"),
                jarBundle("oak-jcr.jar"),
                jarBundle("tika-core.jar"));
    }

    private UrlProvisionOption jarBundle(String jar)
            throws MalformedURLException {
        File target = new File("target");
        File bundles = new File(target, "test-bundles");
        return bundle(new File(bundles, jar).toURI().toURL().toString());
    }

    @Inject
    private MicroKernel kernel;

    @Test
    public void testMicroKernel() {
        assertNotNull(kernel);
        assertTrue(Pattern.matches("[0-9a-f]+", kernel.getHeadRevision()));
    }

    @Inject
    private ContentRepository oakRepository;

    @Test
    public void testOakRepository() {
        assertNotNull(oakRepository);
        // TODO: try something with oakRepository
    }

    @Inject
    private Repository jcrRepository;

    @Test
    public void testJcrRepository() {
        assertNotNull(jcrRepository);
        // TODO: try something with jcrRepository
    }

}
