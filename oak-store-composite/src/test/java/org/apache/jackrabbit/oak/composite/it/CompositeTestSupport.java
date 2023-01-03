/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.composite.it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.sling.testing.paxexam.SlingOptions;
import org.apache.sling.testing.paxexam.TestSupport;
import org.junit.BeforeClass;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.ModifiableCompositeOption;

import static org.apache.sling.testing.paxexam.SlingOptions.scr;
import static org.apache.sling.testing.paxexam.SlingOptions.slingCommonsMetrics;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.frameworkProperty;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.CoreOptions.vmOption;

/**
 * PaxExam support for oak-store-composite integration tests.
 * 
 * WARNING: when running the integration tests in an IDE, make sure to generate the oak-store-composite JAR first, otherwise your latest
 * code changes will be ignored. Just run `mvn package` to generate the JAR.
 */
public abstract class CompositeTestSupport extends TestSupport {

    private static final String JACKRABBIT_GROUP_ID = "org.apache.jackrabbit";

    protected abstract Path getConfigDir();

    @Override
    protected ModifiableCompositeOption baseConfiguration() {
        String bundlePath = System.getProperty("bundle.filename");
        if (bundlePath == null) {
            throw new IllegalArgumentException("This IT has to be called with system property 'bundle.filename' set to the path of this bundle jar");
        }
        if (!Files.exists(Paths.get(bundlePath))) {
            throw new IllegalArgumentException("The bundle jar to be tested needs to be built first with 'mvn package'");
        }
        return composite(
            super.baseConfiguration(),
            logging("INFO"),
            junitBundles(),
            felixFileInstall(),
            oak(),
            testBundle("bundle.filename"),
            frameworkProperty("repository.home").value("target")
        );
    }

    public static Option oak() {
        return composite(
            scr(),
            slingCommonsMetrics(),
            jackrabbit(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-commons").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-api").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-blob").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-blob-plugins").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-core").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-core-spi").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-jackrabbit-api").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-query-spi").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-security-spi").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-segment-tar").versionAsInProject(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-store-spi").versionAsInProject(),
            mavenBundle().groupId("com.google.guava").artifactId("guava").versionAsInProject()
        );
    }

    public static Option jackrabbit() {
        return composite(
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("jackrabbit-data").version("2.20.4"),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("jackrabbit-jcr-commons").version("2.20.4"),
            mavenBundle().groupId("javax.jcr").artifactId("jcr").versionAsInProject(),
            mavenBundle().groupId("commons-codec").artifactId("commons-codec").versionAsInProject(),
            mavenBundle().groupId("commons-io").artifactId("commons-io").versionAsInProject()
        );
    }

    protected static Option logging(String level) {
        return composite(
            mavenBundle("org.ops4j.pax.logging", "pax-logging-api", "1.11.13"),
            systemProperty("org.ops4j.pax.logging.DefaultServiceLog.level").value(level),
            SlingOptions.logback()
        );
    }

    protected Option felixFileInstall() {
        return composite(
            mavenBundle("org.apache.felix", "org.apache.felix.configadmin", "1.9.22"),
            mavenBundle("org.apache.felix", "org.apache.felix.fileinstall", "3.7.4"),
            systemProperty("felix.fileinstall.dir").value(getConfigDir().toAbsolutePath().toString()),
            systemProperty("felix.fileinstall.enableConfigSave").value("false"),
            systemProperty("felix.fileinstall.noInitialDelay").value("true")
        );
    }

    /**
     * to debug a test, add this configuration and "run" the test which would block due to suspend="y"
     * then run remote debugger with specified port
     */
    protected static Option debugConfiguration() {
        return vmOption("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005");
    }
}
