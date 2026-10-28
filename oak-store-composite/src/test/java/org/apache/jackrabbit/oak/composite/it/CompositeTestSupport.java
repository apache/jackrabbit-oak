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
import java.util.UUID;

import org.apache.sling.testing.paxexam.SlingOptions;
import org.ops4j.pax.exam.CoreOptions;
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
public abstract class CompositeTestSupport {

    private static final String JACKRABBIT_GROUP_ID = "org.apache.jackrabbit";

    protected abstract Path getConfigDir();

    protected ModifiableCompositeOption baseConfiguration() {
        String bundlePath = System.getProperty("bundle.filename");
        if (bundlePath == null) {
            throw new IllegalArgumentException("This IT has to be called with system property 'bundle.filename' set to the path of this bundle jar");
        }
        if (!Files.exists(Paths.get(bundlePath))) {
            throw new IllegalArgumentException("The bundle jar to be tested needs to be built first with 'mvn package'");
        }
        String localRepository = System.getProperty("maven.repo.local", "");
        return composite(
            systemProperty("pax.exam.osgi.unresolved.fail").value("true"),
            CoreOptions.keepCaches(),
            CoreOptions.when(!localRepository.isBlank()).useOptions(
                systemProperty("org.ops4j.pax.url.mvn.localRepository").value(localRepository)),
            CoreOptions.repository("https://repository.apache.org/snapshots/").id("apache-snapshots").allowSnapshots(),
            CoreOptions.workingDirectory(Paths.get("target", "paxexam", getClass().getSimpleName(),
                UUID.randomUUID().toString()).toAbsolutePath().toString()),
            SlingOptions.backing(),
            spiflyOptions(),
            jpmsOptions(),
            logging("INFO"),
            junitBundles(),
            felixFileInstall(),
            oak(),
            CoreOptions.bundle(Paths.get(bundlePath).toUri().toString()),
            frameworkProperty("repository.home").value("target")
        );
    }

    public static Option oak() {
        return composite(
            scr(),
            slingCommonsMetrics(),
            jackrabbit(),
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-shaded-guava").versionAsInProject(),
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
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("oak-store-spi").versionAsInProject()
        );
    }

    public static Option jackrabbit() {
        return composite(
            mavenBundle().groupId(JACKRABBIT_GROUP_ID).artifactId("jackrabbit-jcr-commons").versionAsInProject(),
            mavenBundle().groupId("javax.jcr").artifactId("jcr").versionAsInProject(),
            mavenBundle().groupId("commons-codec").artifactId("commons-codec").versionAsInProject(),
            mavenBundle().groupId("commons-io").artifactId("commons-io").versionAsInProject(),
            mavenBundle().groupId("org.apache.commons").artifactId("commons-collections4").versionAsInProject(),
            mavenBundle().groupId("org.apache.commons").artifactId("commons-lang3").versionAsInProject(),
            mavenBundle().groupId("org.apache.commons").artifactId("commons-text").versionAsInProject(),
            mavenBundle().groupId("com.github.ben-manes.caffeine").artifactId("caffeine").versionAsInProject(),
            mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-core").versionAsInProject(),
            mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-annotations").versionAsInProject(),
            mavenBundle().groupId("com.fasterxml.jackson.core").artifactId("jackson-databind").versionAsInProject()
        );
    }

    protected static Option logging(String level) {
        return composite(
            mavenBundle("org.ops4j.pax.logging", "pax-logging-api", "1.11.13"),
            mavenBundle().groupId("org.slf4j").artifactId("slf4j-api").versionAsInProject(),
            mavenBundle().groupId("ch.qos.logback").artifactId("logback-core").versionAsInProject(),
            mavenBundle().groupId("ch.qos.logback").artifactId("logback-classic").versionAsInProject(),
            systemProperty("org.ops4j.pax.logging.DefaultServiceLog.level").value(level),
            systemProperty("logback.configurationFile").value(
                Paths.get("src", "test", "resources", "logback.xml").toUri().toString())
        );
    }

    protected static Option spiflyOptions() {
        return composite(
            mavenBundle("org.apache.aries", "org.apache.aries.util", "1.1.3"),
            mavenBundle("org.ow2.asm", "asm", "9.7.1"),
            mavenBundle("org.ow2.asm", "asm-tree", "9.7.1"),
            mavenBundle("org.ow2.asm", "asm-analysis", "9.7.1"),
            mavenBundle("org.ow2.asm", "asm-commons", "9.7.1"),
            mavenBundle("org.ow2.asm", "asm-util", "9.7.1"),
            mavenBundle("org.apache.aries.spifly", "org.apache.aries.spifly.dynamic.bundle", "1.3.7")
        );
    }

    protected static Option jpmsOptions() {
        return composite(
            vmOption("--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.lang=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.io=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.net=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.nio=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.util=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.util.jar=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.util.regex=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/java.util.zip=ALL-UNNAMED"),
            vmOption("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
        );
    }

    protected Option felixFileInstall() {
        return composite(
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
