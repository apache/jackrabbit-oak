/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import javax.inject.Inject;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.Version;
import org.osgi.framework.namespace.PackageNamespace;
import org.osgi.framework.wiring.BundleCapability;
import org.osgi.framework.wiring.BundleRequirement;
import org.osgi.framework.wiring.BundleRevision;
import org.osgi.framework.wiring.BundleWire;
import org.osgi.framework.wiring.BundleWiring;

/**
 * Tests for the logging bridges embedded in the cloud and Elastic bundles.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class EmbeddedLoggingIT {

    @Inject
    private BundleContext context;

    /**
     * Adds the cloud bundle and its HTTP dependencies to the standard Oak runtime.
     */
    @Configuration
    public Option[] configuration() throws IOException, URISyntaxException {
        return CoreOptions.options(
                CoreOptions.composite(new OSGiIT().configuration()),
                OSGiIT.jarBundles(new File("target/test-bundles/cloud")));
    }

    /**
     * Checks library logging contracts and the embedded Elastic client's shared SLF4J SPI.
     */
    @Test
    public void slf4jImportRanges() throws InvalidSyntaxException, ClassNotFoundException {
        Version slf4j17 = new Version("1.7.36");
        Version slf4j20 = new Version("2.0.20");
        Version slf4j30 = new Version("3.0.0");
        int imports = 0;
        for (Bundle bundle : context.getBundles()) {
            if (!bundle.getSymbolicName().startsWith("org.apache.jackrabbit.oak")) {
                continue;
            }
            BundleWiring wiring = bundle.adapt(BundleWiring.class);
            Assert.assertNotNull("SLF4J imports cannot be checked because bundle wiring is unavailable: " + bundle,
                    wiring);
            boolean elastic = "org.apache.jackrabbit.oak-search-elastic".equals(bundle.getSymbolicName());
            boolean cloud = "org.apache.jackrabbit.oak-blob-cloud".equals(bundle.getSymbolicName());
            if (elastic) {
                Class.forName("co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester",
                        true, wiring.getClassLoader());
            }
            for (BundleWire wire : wiring.getRequiredWires(PackageNamespace.PACKAGE_NAMESPACE)) {
                String packageName = (String) wire.getCapability().getAttributes()
                        .get(PackageNamespace.PACKAGE_NAMESPACE);
                if (!packageName.equals("org.slf4j") && !packageName.startsWith("org.slf4j.")) {
                    continue;
                }
                Filter filter = FrameworkUtil.createFilter(wire.getRequirement().getDirectives()
                        .get(PackageNamespace.REQUIREMENT_FILTER_DIRECTIVE));
                String message = bundle + ": " + filter;
                Assert.assertTrue(message, filter.matches(Map.of(
                        PackageNamespace.PACKAGE_NAMESPACE, packageName,
                        PackageNamespace.CAPABILITY_VERSION_ATTRIBUTE, slf4j17)));
                boolean supportsSlf4j2 = elastic && packageName.equals("org.slf4j.spi")
                        || cloud && packageName.equals("org.slf4j.event");
                Assert.assertEquals(message, supportsSlf4j2, filter.matches(Map.of(
                        PackageNamespace.PACKAGE_NAMESPACE, packageName,
                        PackageNamespace.CAPABILITY_VERSION_ATTRIBUTE, slf4j20)));
                Assert.assertFalse(message, filter.matches(Map.of(
                        PackageNamespace.PACKAGE_NAMESPACE, packageName,
                        PackageNamespace.CAPABILITY_VERSION_ATTRIBUTE, slf4j30)));
                imports++;
            }
        }
        Assert.assertTrue("No Oak SLF4J imports were checked", imports > 0);
    }

    /**
     * Checks the embedded AWS SDK's logging requirements against SLF4J alone, without Pax Logging exports.
     */
    @Test
    public void cloudSlf4jImports() throws InvalidSyntaxException {
        Bundle provider = getBundle("slf4j.api");
        List<BundleCapability> exports = provider.adapt(BundleRevision.class)
                .getDeclaredCapabilities(PackageNamespace.PACKAGE_NAMESPACE);
        Bundle cloud = getBundle("org.apache.jackrabbit.oak-blob-cloud");
        Assert.assertEquals(cloud.toString(), Bundle.ACTIVE, cloud.getState());
        boolean eventChecked = false;
        for (BundleRequirement requirement : cloud.adapt(BundleRevision.class)
                .getDeclaredRequirements(PackageNamespace.PACKAGE_NAMESPACE)) {
            String filterText = requirement.getDirectives()
                    .get(PackageNamespace.REQUIREMENT_FILTER_DIRECTIVE);
            if (!filterText.contains("(osgi.wiring.package=org.slf4j)")
                    && !filterText.contains("(osgi.wiring.package=org.slf4j.")) {
                continue;
            }
            boolean optional = PackageNamespace.RESOLUTION_OPTIONAL.equals(requirement.getDirectives()
                    .get(PackageNamespace.REQUIREMENT_RESOLUTION_DIRECTIVE));
            String message = cloud + ": " + filterText + " against " + provider;
            if (filterText.contains("(osgi.wiring.package=org.slf4j.event)")) {
                Assert.assertFalse(message, optional);
                Filter filter = FrameworkUtil.createFilter(filterText);
                Assert.assertFalse(message, filter.matches(Map.of(
                        PackageNamespace.PACKAGE_NAMESPACE, "org.slf4j.event",
                        PackageNamespace.CAPABILITY_VERSION_ATTRIBUTE, new Version("3.0.0"))));
                eventChecked = true;
            }
            if (!optional) {
                Assert.assertTrue(message, exports.stream().anyMatch(requirement::matches));
            }
        }
        Assert.assertTrue("No AWS SDK SLF4J event import was checked", eventChecked);
    }

    /**
     * Verifies the embedded Commons Logging bridge and AWS logging against the selected runtime.
     */
    @Test
    public void embeddedSlf4jLogging() throws ReflectiveOperationException {
        Bundle api = getBundle("slf4j.api");
        Bundle elastic = getBundle("org.apache.jackrabbit.oak-search-elastic");
        Bundle cloud = getBundle("org.apache.jackrabbit.oak-blob-cloud");
        Class<?> loggerFactory = api.loadClass("org.slf4j.LoggerFactory");
        Assert.assertSame(loggerFactory, elastic.loadClass("org.slf4j.LoggerFactory"));
        Assert.assertSame(loggerFactory, cloud.loadClass("org.slf4j.LoggerFactory"));
        LoggerContext loggerContext = (LoggerContext) loggerFactory.getMethod("getILoggerFactory").invoke(null);
        Logger logger = loggerContext.getLogger(EmbeddedLoggingIT.class.getName() + ".embedded");
        Object commonsLog = commonsLogger(elastic, logger.getName());
        Object sdkLog = awsLogger(cloud, logger);
        Class<?> logClass = elastic.loadClass("org.apache.commons.logging.Log");
        Class<?> sdkClass = sdkLog.getClass();
        Class<?> levelClass = api.loadClass("org.slf4j.event.Level");
        Assert.assertSame(levelClass, cloud.loadClass("org.slf4j.event.Level"));
        Method sdkEnabled = sdkClass.getMethod("isLoggingLevelEnabled", levelClass);
        Method sdkLogging = sdkClass.getMethod("log", levelClass, Supplier.class);
        Level previousLevel = logger.getLevel();
        boolean previousAdditive = logger.isAdditive();
        ListAppender<ILoggingEvent> appender = new ListAppender<>() {
            @Override
            protected void append(ILoggingEvent event) {
                // Capture caller data while the logging call is still on the stack.
                event.getCallerData();
                super.append(event);
            }
        };
        appender.start();
        logger.addAppender(appender);
        logger.setAdditive(false);
        logger.setLevel(Level.TRACE);
        try {
            for (String level : List.of("TRACE", "DEBUG", "INFO", "WARN", "ERROR")) {
                String method = level.toLowerCase(Locale.ROOT);
                String enabledMethod = "is" + level.charAt(0) + method.substring(1) + "Enabled";
                Method commonsEnabled = logClass.getMethod(enabledMethod);
                Method commonsLogging = logClass.getMethod(method, Object.class);
                Method commonsFailure = logClass.getMethod(method, Object.class, Throwable.class);
                Method sdkFailure = sdkClass.getMethod(method, Supplier.class, Throwable.class);
                Object sdkLevel = levelClass.getField(level).get(null);
                Level expectedLevel = Level.toLevel(level);
                appender.list.clear();
                Assert.assertEquals(true, commonsEnabled.invoke(commonsLog));
                commonsLogging.invoke(commonsLog, "commons-" + level);
                Assert.assertEquals(true, sdkEnabled.invoke(sdkLog, sdkLevel));
                Supplier<String> message = () -> "aws-" + level;
                sdkLogging.invoke(sdkLog, sdkLevel, message);
                Assert.assertEquals(2, appender.list.size());
                assertLogEvent(appender.list.get(0), "commons-" + level, expectedLevel);
                assertLogEvent(appender.list.get(1), "aws-" + level, expectedLevel);
                StackTraceElement caller = Arrays.stream(appender.list.get(0).getCallerData())
                        .filter(frame -> !frame.getClassName().startsWith("java.lang.reflect.")
                                && !frame.getClassName().startsWith("jdk.internal.reflect."))
                        .findFirst().orElseThrow(() -> new AssertionError("Missing bridge caller data"));
                Assert.assertEquals(EmbeddedLoggingIT.class.getName(), caller.getClassName());
                Assert.assertEquals("embeddedSlf4jLogging", caller.getMethodName());

                Throwable failure = new IllegalStateException("logging-failure-" + level);
                commonsFailure.invoke(commonsLog, "commons-failure-" + level, failure);
                Supplier<String> failureMessage = () -> "aws-failure-" + level;
                sdkFailure.invoke(sdkLog, failureMessage, failure);
                Assert.assertEquals(4, appender.list.size());
                assertLogEvent(appender.list.get(2), "commons-failure-" + level, expectedLevel);
                assertLogEvent(appender.list.get(3), "aws-failure-" + level, expectedLevel);
                Assert.assertSame(failure, ((ThrowableProxy) appender.list.get(2).getThrowableProxy()).getThrowable());
                Assert.assertSame(failure, ((ThrowableProxy) appender.list.get(3).getThrowableProxy()).getThrowable());

                logger.setLevel(Level.OFF);
                appender.list.clear();
                Assert.assertEquals(false, commonsEnabled.invoke(commonsLog));
                commonsLogging.invoke(commonsLog, "suppressed");
                commonsFailure.invoke(commonsLog, "suppressed", failure);
                Assert.assertEquals(false, sdkEnabled.invoke(sdkLog, sdkLevel));
                Supplier<String> suppressed = () -> {
                    throw new AssertionError("Disabled AWS logging evaluated its message supplier at " + level);
                };
                sdkLogging.invoke(sdkLog, sdkLevel, suppressed);
                sdkFailure.invoke(sdkLog, suppressed, failure);
                Assert.assertTrue(appender.list.isEmpty());
                logger.setLevel(Level.TRACE);
            }
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
            logger.setAdditive(previousAdditive);
            appender.stop();
        }
    }

    private Bundle getBundle(String symbolicName) {
        return Arrays.stream(context.getBundles())
                .filter(bundle -> symbolicName.equals(bundle.getSymbolicName()))
                .findFirst().orElseThrow(() -> new IllegalStateException("Missing provisioned bundle: " + symbolicName));
    }

    private Object commonsLogger(Bundle elastic, String loggerName) throws ReflectiveOperationException {
        Class<?> logFactory = elastic.loadClass("org.apache.commons.logging.LogFactory");
        Object factory = logFactory.getMethod("getFactory").invoke(null);
        Assert.assertEquals("org.apache.commons.logging.impl.Slf4jLogFactory", factory.getClass().getName());
        return logFactory.getMethod("getLog", String.class).invoke(null, loggerName);
    }

    private Object awsLogger(Bundle cloud, Logger logger) throws ReflectiveOperationException {
        Class<?> sdkClass = cloud.loadClass("software.amazon.awssdk.utils.Logger");
        Object sdkLog = sdkClass.getMethod("loggerFor", String.class).invoke(null, logger.getName());
        Assert.assertSame(logger, sdkClass.getMethod("logger").invoke(sdkLog));
        return sdkLog;
    }

    private void assertLogEvent(ILoggingEvent event, String message, Level level) {
        Assert.assertEquals(message, event.getFormattedMessage());
        Assert.assertEquals(level, event.getLevel());
    }
}
