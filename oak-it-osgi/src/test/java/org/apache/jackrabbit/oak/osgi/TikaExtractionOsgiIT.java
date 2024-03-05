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

import org.apache.sling.testing.paxexam.SlingOptions;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xml.sax.ContentHandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.Arrays.stream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.frameworkProperty;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.systemProperties;
import static org.ops4j.pax.exam.CoreOptions.wrappedBundle;

import static org.apache.jackrabbit.oak.osgi.OSGiIT.getConfigDir;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class TikaExtractionOsgiIT {
    private static final Logger LOG = LoggerFactory.getLogger(TikaExtractionOsgiIT.class);

    private static final String VERSION_PROP_RESOURCE_NAME = "versions.properties";
    private static final String TIKA_VERSION = "tika";
    private static final String POI_VERSION = "poi";
    private static final String COLLECTIONS4_VERSION = "commons-collections4";
    private static final String COMPRESS_VERSION = "commons-compress";
    private static final String LANG3_VERSION = "commons-lang3";
    private static final String MATH3_VERSION = "commons-math3";
    private static final String[] VERSION_KEYS = new String[]{TIKA_VERSION, POI_VERSION
            , COLLECTIONS4_VERSION, COMPRESS_VERSION
            , LANG3_VERSION, MATH3_VERSION};

    private static final String EXPECTED_TEXT_FRAGMENT = "A sample document";

    @Configuration
    public Option[] configuration() throws IOException {
        return CoreOptions.options(
                junitBundles(),
                mavenBundle("org.apache.felix", "org.apache.felix.scr", "2.1.28"),
                // transitive deps of Felix SCR 2.1.x
                mavenBundle("org.osgi", "org.osgi.util.promise", "1.1.1"),
                mavenBundle("org.osgi", "org.osgi.util.function", "1.1.0"),
                mavenBundle("org.apache.felix", "org.apache.felix.jaas", "1.0.2"),
                mavenBundle("org.osgi", "org.osgi.dto", "1.0.0"),
                // require at least ConfigAdmin 1.6 supported by felix.configadmin 1.9.0+
                mavenBundle( "org.apache.felix", "org.apache.felix.configadmin", "1.9.20" ),
                mavenBundle( "org.apache.felix", "org.apache.felix.fileinstall", "3.2.6" ),
                mavenBundle( "org.ops4j.pax.logging", "pax-logging-api", "1.7.2" ),
                mavenBundle("org.apache.logging.log4j", "log4j-api", "2.23.0"),
                mavenBundle("jakarta.servlet", "jakarta.servlet-api", "5.0.0"),
                SlingOptions.spyfly(),
                setupTikaAndPoi(),
                frameworkProperty("repository.home").value("target"),
                systemProperties(new SystemPropertyOption("felix.fileinstall.dir").value(getConfigDir())),
                OSGiIT.jpmsOptions()
                // to debug a test, un-comment this and "run" the test which would block due to suspend="y"
                // then run debugger on a remote app with specified port
//                , vmOption( "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005" )
        );
    }

    private static Option setupTikaAndPoi() throws IOException {
        Map<String, String> versions = setupVersions();
        return composite(
                composite(
                        mavenBundle("org.apache.tika", "tika-core", versions.get(TIKA_VERSION))
                        , mavenBundle("org.apache.tika", "tika-parsers", versions.get(TIKA_VERSION))

                        // poi dependency start
                        , wrappedBundle(mavenBundle("org.apache.poi", "poi", versions.get(POI_VERSION)))
                        , wrappedBundle(mavenBundle("org.apache.poi", "poi-scratchpad", versions.get(POI_VERSION)))
                        , wrappedBundle(mavenBundle("org.apache.poi", "poi-ooxml", versions.get(POI_VERSION)))
                        , wrappedBundle(mavenBundle("org.apache.poi", "poi-ooxml-lite", versions.get(POI_VERSION)))
                                .instructions("DynamicImport-Package=*")
                        , wrappedBundle(mavenBundle("org.apache.poi", "ooxml-security", "1.0"))
                        , wrappedBundle(mavenBundle("org.apache.xmlbeans", "xmlbeans", "5.0.3"))
                        , wrappedBundle(mavenBundle("com.drewnoakes", "metadata-extractor", "2.6.2"))
                        , mavenBundle("org.apache.commons", "commons-collections4", versions.get(COLLECTIONS4_VERSION))
                        , mavenBundle("org.apache.commons", "commons-compress", versions.get(COMPRESS_VERSION))
                        , mavenBundle("org.apache.commons", "commons-lang3", versions.get(LANG3_VERSION))
                        , mavenBundle("org.apache.commons", "commons-math3", versions.get(MATH3_VERSION))
                        // poi dependency end
                )
                , OSGiIT.jarBundles()
        );
    }

    private static Map<String, String> setupVersions() throws IOException {
        Properties props = new Properties();
        props.load(TikaExtractionOsgiIT.class.getClassLoader().getResourceAsStream(VERSION_PROP_RESOURCE_NAME));

        assertEquals("Unexpected number of properties found in " + VERSION_PROP_RESOURCE_NAME,
                VERSION_KEYS.length, props.size());

        Map<String, String> versions = new HashMap<>();
        for (String versionKey : VERSION_KEYS) {
            String version = props.getProperty(versionKey);

            assertNotNull("Version value not found for " + versionKey, version);
            assertFalse("Version (key: " + versionKey + ", value: " + version + ") didn't get filtered by maven",
                    version.contains("{"));

            versions.put(versionKey, version);
        }

        return versions;
    }

    @Inject
    private BundleContext context;

    @Inject
    private Parser registeredParser;

    @Test
    public void listBundles() {
        for (Bundle bundle : context.getBundles()) {
            LOG.info("Bundle listing :: {} - {}", bundle, bundle.getVersion());
        }
    }

    @Test
    public void doc() throws Exception {
        assertFileContains("test.doc");
    }

    @Test
    public void docx() throws Exception {
        assertFileContains("test.docx");
    }

    @Test
    public void rtf() throws Exception {
        assertFileContains("test.rtf");
    }

    @Test
    public void text() throws Exception {
        assertFileContains("test.txt");
    }

    private void assertFileContains(String resName) throws Exception {
        AutoDetectParser parser = new AutoDetectParser(registeredParser);
        ContentHandler handler = new WriteOutContentHandler();
        Metadata metadata = new Metadata();

        InputStream stream = getClass().getResourceAsStream(resName);
        assertNotNull("Input stream must not be null", stream);
        try {
            parser.parse(stream, handler, metadata);

            String actual = handler.toString().trim();
            assertEquals(EXPECTED_TEXT_FRAGMENT, actual);
        } finally {
            stream.close();
        }

    }
}
