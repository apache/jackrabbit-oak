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
package org.apache.jackrabbit.oak.upgrade.cli.parser;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.OakUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StoreArgumentsTest {

    private static final List<String> RETIRED_OPTIONS = List.of(
            "--early-shutdown", "--fail-on-error", "--skip-init", "--skip-name-check"
    );

    private static final String RETIRED_OPTIONS_WARNING = "Warning: ignoring retired Jackrabbit Classic options:";

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testSameRepositoryFails() throws CliArgumentException {
        MigrationCliArguments parsed = new MigrationCliArguments(OptionParserFactory.create().parse("my/repo", "my/repo"));
        MigrationOptions options = new MigrationOptions(parsed);
        CliArgumentException error = Assert.assertThrows(CliArgumentException.class,
                () -> new StoreArguments(options, parsed.getArguments()));
        Assert.assertTrue(error.getMessage().contains("same repository"));
    }

    /** Old commands still copy Oak content and report all ignored flags in one warning. */
    @Test
    public void testRetiredFlagsDoNotPreventSidegrade() throws Exception {
        NodeStoreContainer source = new SegmentTarNodeStoreContainer(tmp.newFolder("oak-source"));
        populateOakSource(source);
        File destination = new File(tmp.getRoot(), "destination");
        List<String> args = new ArrayList<>(RETIRED_OPTIONS);
        args.add("--skip-init");
        args.add(source.getDescription());
        args.add(destination.getPath());

        String output = runCli(0, args.toArray(String[]::new));

        List<String> warnings = output.lines().filter(line -> line.startsWith(RETIRED_OPTIONS_WARNING)).toList();
        Assert.assertEquals(output, List.of(RETIRED_OPTIONS_WARNING + " " + String.join(", ", RETIRED_OPTIONS) + "."), warnings);
        Assert.assertTrue(output, output.contains("https://repo.maven.apache.org/maven2/org/apache/jackrabbit/"
                + "oak-upgrade/2.8.0/oak-upgrade-2.8.0.jar"));
        assertCopiedTo(destination);
    }

    /** A typo in an option is not mistaken for a retired Classic flag. */
    @Test
    public void testUnrelatedOptionKeepsParserError() throws Exception {
        File destination = new File(tmp.getRoot(), "destination");
        String output = runCli(1, "--skip-init", "--not-an-oak-option", "source", destination.getPath());
        Assert.assertTrue(output, output.contains("UnrecognizedOptionException"));
        Assert.assertFalse(output, output.contains(RETIRED_OPTIONS_WARNING));
        Assert.assertFalse(destination.exists());
    }

    /** Only parsed flags, not values or operands after the delimiter, trigger a warning. */
    @Test
    public void testFlagLikeOperandsDoNotTriggerWarning() throws Exception {
        String output = runCli(0, "--help", "--src-user", "--skip-init", "--", "--early-shutdown");
        Assert.assertTrue(output, output.contains("Usage:"));
        Assert.assertFalse(output, output.contains(RETIRED_OPTIONS_WARNING));
    }

    /** Accepting the retired flags does not re-enable Classic migration. */
    @Test
    public void testClassicSourceWithRetiredFlagStillFails() throws Exception {
        File source = tmp.newFolder("classic-source");
        Files.createDirectory(source.toPath().resolve("workspaces"));
        File destination = new File(tmp.getRoot(), "destination");

        String output = runCli(1, "--skip-init", source.getPath(), destination.getPath());

        Assert.assertTrue(output, output.contains("Jackrabbit Classic sources are not supported"));
        Assert.assertFalse(destination.exists());
    }

    @Test
    public void testHelpOnlyAdvertisesSidegrade() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        try (PrintStream helpOut = new PrintStream(output, false, StandardCharsets.UTF_8)) {
            System.setOut(helpOut);
            OakUpgrade.main("--help");
        } finally {
            System.setOut(originalOut);
        }
        String help = output.toString(StandardCharsets.UTF_8);
        Assert.assertTrue(help.contains("Usage: java -jar oak-upgrade-*.jar [options] source destination"));
        Assert.assertTrue(help.contains("segment-old:"));
        Assert.assertFalse(help.contains("jcr2_source"));
        for (String option : RETIRED_OPTIONS) {
            Assert.assertFalse(help.contains(option));
        }
    }

    @Test
    public void testXmlInputIsRejectedWithoutCreatingDestination() throws Exception {
        File xml = tmp.newFile("custom-config.XML");
        File destination = new File(tmp.getRoot(), "destination");
        assertClassicMigrationRejected(xml.getPath(), destination.getPath());
        Assert.assertFalse(destination.exists());
        assertClassicMigrationRejected(destination.getPath(), xml.getPath());
        Assert.assertFalse(destination.exists());
    }

    @Test
    public void testClassicHomesAreRejectedWithoutCreatingDestination() throws Exception {
        File classicWithConfig = tmp.newFolder("classic-with-config");
        Files.createFile(classicWithConfig.toPath().resolve("repository.xml"));
        Files.createDirectory(classicWithConfig.toPath().resolve("segmentstore"));
        File classicWithWorkspaces = tmp.newFolder("classic-with-workspaces");
        Files.createDirectory(classicWithWorkspaces.toPath().resolve("workspaces"));
        File destination = new File(tmp.getRoot(), "destination");

        assertClassicMigrationRejected(classicWithConfig.getPath(), destination.getPath());
        assertClassicMigrationRejected("segment-old:" + classicWithWorkspaces.getPath(), destination.getPath());
        assertClassicMigrationRejected(classicWithWorkspaces.getPath());
        Assert.assertFalse(destination.exists());
    }

    /** Reject Classic homes in the destination slot before creating an Oak store. */
    @Test
    public void testClassicDestinationsAreRejectedWithoutCreatingSegmentStore() throws Exception {
        NodeStoreContainer source = new SegmentTarNodeStoreContainer(tmp.newFolder("oak-source"));
        populateOakSource(source);
        File classicWithConfig = tmp.newFolder("classic-with-config");
        Files.createFile(classicWithConfig.toPath().resolve("repository.xml"));
        File classicWithWorkspaces = tmp.newFolder("classic-with-workspaces");
        Files.createDirectory(classicWithWorkspaces.toPath().resolve("workspaces"));

        assertClassicDestinationRejected(source.getDescription(), classicWithConfig.getPath());
        assertClassicDestinationRejected("--src-external-ds=false", source.getDescription(),
                StoreArguments.SEGMENT_OLD_PREFIX + classicWithWorkspaces.getPath());
        Assert.assertFalse(new File(classicWithConfig, "segmentstore").exists());
        Assert.assertFalse(new File(classicWithWorkspaces, "segmentstore").exists());
    }

    @Test
    public void testThreeArgumentSyntaxIsRejectedWithoutCreatingDestination() throws Exception {
        File home = tmp.newFolder("classic");
        Files.createDirectory(home.toPath().resolve("workspaces"));
        File xml = tmp.newFile("repository.xml");
        File destination = new File(tmp.getRoot(), "destination");
        assertClassicMigrationRejected(home.getPath(), xml.getPath(), destination.getPath());
        Assert.assertFalse(destination.exists());
    }

    @Test
    public void testMissingAndEmptySegmentSourcesAreNotCreated() {
        File missing = new File(tmp.getRoot(), "missing");
        File destination = new File(tmp.getRoot(), "destination");
        File segmentstore = new File(tmp.getRoot(), "segmentstore");

        assertInvalidSegmentSource(missing.getPath(), destination.getPath());
        assertInvalidSegmentSource(tmp.getRoot().getPath(), destination.getPath());
        Assert.assertFalse(missing.exists());
        Assert.assertFalse(segmentstore.exists());
        Assert.assertFalse(destination.exists());
    }

    @Test
    public void testSidegradeSegmentTarHomeWithClassicArtifacts() throws Exception {
        File home = tmp.newFolder("repository.xml");
        assertSidegradeWithClassicArtifacts(new SegmentTarNodeStoreContainer(home), home);
    }

    @Test
    public void testSidegradeOldSegmentHomeWithClassicArtifacts() throws Exception {
        File home = tmp.newFolder("old-segment-source");
        assertSidegradeWithClassicArtifacts(new SegmentNodeStoreContainer(home), home);
    }

    /** Leftover Classic files do not prevent writes to an existing Oak store. */
    @Test
    public void testSidegradeToOakDestinationWithClassicArtifacts() throws Exception {
        NodeStoreContainer source = new SegmentTarNodeStoreContainer(tmp.newFolder("oak-source"));
        populateOakSource(source);
        File home = tmp.newFolder("oak-destination");
        try (NodeStoreContainer target = new SegmentTarNodeStoreContainer(home)) {
            target.open();
        }
        Files.createFile(home.toPath().resolve("repository.xml"));
        Files.createDirectory(home.toPath().resolve("workspaces"));

        OakUpgrade.migrate(parse(source.getDescription(), home.getPath()));
        assertCopiedTo(home);
    }

    private void assertSidegradeWithClassicArtifacts(NodeStoreContainer source, File home) throws Exception {
        populateOakSource(source);
        Files.createFile(home.toPath().resolve("repository.xml"));
        Files.createDirectory(home.toPath().resolve("workspaces"));

        File destination = new File(tmp.getRoot(), "new-destination");
        OakUpgrade.migrate(parse("--src-external-ds=false", source.getDescription(), destination.getPath()));
        assertCopiedTo(destination);
    }

    private static void assertCopiedTo(File destination) throws Exception {
        try (NodeStoreContainer target = new SegmentTarNodeStoreContainer(destination)) {
            Assert.assertEquals("copied", target.open().getRoot().getChildNode("sidegrade-test").getString("value"));
        }
    }

    private static void populateOakSource(NodeStoreContainer source) throws Exception {
        try (source) {
            NodeStore store = source.open();
            NodeBuilder root = store.getRoot().builder();
            root.child("sidegrade-test").setProperty("value", "copied");
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    private static void assertClassicDestinationRejected(String... args) {
        CliArgumentException error = Assert.assertThrows(CliArgumentException.class, () -> OakUpgrade.migrate(parse(args)));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("Jackrabbit Classic destinations are not supported"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("Oak 2.8.0"));
    }

    private String runCli(int expectedExitCode, String... args) throws Exception {
        File java = new File(System.getProperty("java.home"), "bin/java");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        File output = tmp.newFile();
        List<String> command = new ArrayList<>(List.of(java.getPath(), "-cp", classpath, OakUpgrade.class.getName()));
        command.addAll(List.of(args));
        try (Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).redirectOutput(output).start()) {
            try {
                Assert.assertTrue("CLI timed out", process.waitFor(30, TimeUnit.SECONDS));
                String message = Files.readString(output.toPath());
                Assert.assertEquals(message, expectedExitCode, process.exitValue());
                return message;
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        }
    }

    private static void assertClassicMigrationRejected(String... descriptors) {
        String[] args = new String[descriptors.length + 1];
        args[0] = "--src-external-ds=false";
        System.arraycopy(descriptors, 0, args, 1, descriptors.length);
        CliArgumentException error = Assert.assertThrows(CliArgumentException.class, () -> OakUpgrade.migrate(parse(args)));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("Oak 2.8.0"));
    }

    private static void assertInvalidSegmentSource(String source, String destination) {
        CliArgumentException error = Assert.assertThrows(CliArgumentException.class,
                () -> OakUpgrade.migrate(parse("--src-external-ds=false", source, destination)));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("segmentstore"));
    }

    private static MigrationCliArguments parse(String... args) {
        return new MigrationCliArguments(OptionParserFactory.create().parse(args));
    }
}
