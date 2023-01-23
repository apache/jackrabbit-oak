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
package org.apache.jackrabbit.oak.run;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.stream.Stream;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.LIST;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DataStoreCopyCommandTest {

    @ClassRule
    public static AzuriteDockerRule AZURITE = new AzuriteDockerRule();

    private static final String BLOB1 = "8897-b9025dc534d4a9fa5920569b373cd714c8cfe5d030ca3f5edb25004894a5";
    private static final String BLOB2 = "c1f0-8893512e1e00910a9caff05487805632031f15a0bd8ee869c9205da59cb8";
    private static final ImmutableSet<String> BLOBS = ImmutableSet.of(BLOB1, BLOB2);

    @Rule
    public TemporaryFolder outDir = new TemporaryFolder();

    private CloudBlobContainer container;

    @Before
    public void setUp() throws Exception {
        container = createBlobContainer();
    }

    @After
    public void tearDown() throws Exception {
        if (container != null) {
            container.deleteIfExists();
        }
        FileUtils.cleanDirectory(outDir.getRoot());
    }

    @Test(expected = RuntimeException.class)
    public void missingRequiredOptions() throws Exception {
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        cmd.execute(
                "--source-repo",
                container.getUri().toURL().toString()
        );
    }

    @Test(expected = RuntimeException.class)
    public void unauthenticated() throws Exception {
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        cmd.execute(
                "--source-repo",
                container.getUri().toURL().toString(),
                "--include-path",
                BLOB1
        );
    }

    @Test
    public void singleBlobWithIncludePath() throws Exception {
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        cmd.execute(
                "--source-repo",
                container.getUri().toURL().toString(),
                "--include-path",
                BLOB1,
                "--sas-token",
                container.generateSharedAccessSignature(policy(EnumSet.of(READ, LIST)), null),
                "--out-dir",
                outDir.getRoot().getAbsolutePath()
        );

        File outDirRoot = outDir.getRoot();
        String blobName = BLOB1.replaceAll("-", "");
        File firstNode = new File(outDirRoot, blobName.substring(0, 2));
        assertTrue(firstNode.exists() && firstNode.isDirectory());
        File secondNode = new File(firstNode, blobName.substring(2, 4));
        assertTrue(secondNode.exists() && secondNode.isDirectory());
        File thirdNode = new File(secondNode, blobName.substring(4, 6));
        assertTrue(thirdNode.exists() && thirdNode.isDirectory());
        File blob = new File(thirdNode, blobName);
        assertTrue(blob.exists() && blob.isFile());
        assertEquals(BLOB1, IOUtils.toString(blob.toURI(), StandardCharsets.UTF_8));
    }

    @Test
    public void allBlobsWithFileIncludePath() throws Exception {
        Path blobs = Files.createTempFile("blobs", "txt");
        IOUtils.write(BLOB1 + "\n" + BLOB2, Files.newOutputStream(blobs.toFile().toPath()), StandardCharsets.UTF_8);
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        cmd.execute(
                "--source-repo",
                container.getUri().toURL().toString(),
                "--file-include-path",
                blobs.toString(),
                "--sas-token",
                container.generateSharedAccessSignature(policy(EnumSet.of(READ, LIST)), null),
                "--out-dir",
                outDir.getRoot().getAbsolutePath()
        );

        try (Stream<Path> files = Files.walk(outDir.getRoot().toPath()).filter(p -> p.toFile().isFile())) {
            assertEquals(2, files.count());
        }
    }

    @Test
    public void allBlobsPlusMissingOne() throws Exception {
        Path blobs = Files.createTempFile("blobs", "txt");
        IOUtils.write(BLOB1 + "\n" + BLOB2 + "\n" + "foo", Files.newOutputStream(blobs.toFile().toPath()), StandardCharsets.UTF_8);
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        cmd.execute(
                "--source-repo",
                container.getUri().toURL().toString(),
                "--file-include-path",
                blobs.toString(),
                "--sas-token",
                container.generateSharedAccessSignature(policy(EnumSet.of(READ, LIST)), null),
                "--out-dir",
                outDir.getRoot().getAbsolutePath()
        );

        try (Stream<Path> files = Files.walk(outDir.getRoot().toPath()).filter(p -> p.toFile().isFile())) {
            assertEquals(2, files.count());
        }
    }

    @Test
    public void onlyFailures() throws Exception {
        Path blobs = Files.createTempFile("blobs", "txt");
        IOUtils.write("foo" + "\n" + "bar", Files.newOutputStream(blobs.toFile().toPath()), StandardCharsets.UTF_8);
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        assertThrows(RuntimeException.class, () -> cmd.execute(
                "--source-repo",
                container.getUri().toURL().toString(),
                "--file-include-path",
                blobs.toString(),
                "--sas-token",
                container.generateSharedAccessSignature(policy(EnumSet.of(READ, LIST)), null),
                "--out-dir",
                outDir.getRoot().getAbsolutePath()
        ));
    }

    @Test
    public void allBlobsPlusMissingOneWithFailOnError() throws Exception {
        Path blobs = Files.createTempFile("blobs", "txt");
        IOUtils.write(BLOB1 + "\n" + BLOB2 + "\n" + "foo", Files.newOutputStream(blobs.toFile().toPath()), StandardCharsets.UTF_8);
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        assertThrows(RuntimeException.class, () -> cmd.execute(
                "--source-repo",
                container.getUri().toURL().toString(),
                "--file-include-path",
                blobs.toString(),
                "--sas-token",
                container.generateSharedAccessSignature(policy(EnumSet.of(READ, LIST)), null),
                "--out-dir",
                outDir.getRoot().getAbsolutePath(),
                "--fail-on-error",
                "true"
        ));
    }

    @Test
    public void destinationFromBlobId() throws Exception {
        DataStoreCopyCommand cmd = new DataStoreCopyCommand();
        cmd.parseCommandLineParams(
                "--source-repo",
                container.getUri().toURL().toString(),
                "--include-path",
                BLOB1,
                "--out-dir",
                outDir.getRoot().getAbsolutePath()
        );
        assertEquals(Joiner.on(File.separator).join(outDir.getRoot().getAbsolutePath(), "88",
                        "97", "b9","8897b9025dc534d4a9fa5920569b373cd714c8cfe5d030ca3f5edb25004894a5"),
                cmd.getDestinationFromId(BLOB1)
        );
    }

    private CloudBlobContainer createBlobContainer() throws Exception {
        container = AZURITE.getContainer("blobstore");
        for (String blob : BLOBS) {
            container.getBlockBlobReference(blob).uploadText(blob);
        }
        return container;
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions, Instant expirationTime) {
        SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
        sharedAccessBlobPolicy.setPermissions(permissions);
        sharedAccessBlobPolicy.setSharedAccessExpiryTime(Date.from(expirationTime));
        return sharedAccessBlobPolicy;
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions) {
        return policy(permissions, Instant.now().plus(Duration.ofDays(7)));
    }
}
