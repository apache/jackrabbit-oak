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

import static org.junit.Assert.assertEquals;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.frameworkProperty;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.systemProperties;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import javax.inject.Inject;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.options.DefaultCompositeOption;
import org.ops4j.pax.exam.options.SystemPropertyOption;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class OSGiIT {

    @Configuration
    public Option[] configuration() throws IOException, URISyntaxException {
        return CoreOptions.options(
                junitBundles(),
                mavenBundle("org.apache.felix", "org.apache.felix.scr", "2.0.12"),
                mavenBundle("org.osgi", "org.osgi.dto", "1.0.0"),
                mavenBundle( "org.apache.felix", "org.apache.felix.configadmin", "1.8.16" ),
                mavenBundle( "org.apache.felix", "org.apache.felix.fileinstall", "3.2.6" ),
                mavenBundle( "org.ops4j.pax.logging", "pax-logging-api", "1.7.2" ),
                frameworkProperty("repository.home").value("target"),
                systemProperties(new SystemPropertyOption("felix.fileinstall.dir").value(getConfigDir())),
                jarBundles());
    }

    private String getConfigDir(){
        return new File(new File("src", "test"), "config").getAbsolutePath();
    }

    private Option jarBundles() throws MalformedURLException {
        DefaultCompositeOption composite = new DefaultCompositeOption();
        for (File bundle : new File("target", "test-bundles").listFiles()) {
            if (bundle.getName().endsWith(".jar") && bundle.isFile()) {
                composite.add(bundle(bundle.toURI().toURL().toString()));
            }
        }
        return composite;
    }

    @Inject
    private BundleContext context;

    @Test
    public void bundleStates() {
        for (Bundle bundle : context.getBundles()) {
            assertEquals(
                String.format("Bundle %s not active. have a look at the logs", bundle.toString()), 
                Bundle.ACTIVE, bundle.getState());
        }
    }
    
    @Test
    public void listBundles() {
        for (Bundle bundle : context.getBundles()) {
            System.out.println(bundle);
        }
    }

    @Test
    public void listServices() throws InvalidSyntaxException {
        for (ServiceReference reference
                : context.getAllServiceReferences(null, null)) {
            System.out.println(reference);
        }
    }

    @Inject
    private NodeStore store;

    @Test
    public void testNodeStore() {
        System.out.println(store);
        System.out.println(store.getRoot());
    }

    @Inject
    private Repository repository;

    @Test
    public void testRepository() throws RepositoryException {
        System.out.println(repository);
        System.out.println(repository.getDescriptor(Repository.REP_NAME_DESC));
    }

}
