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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * Source-level isolation test that scans all .java files to enforce that:
 * <ul>
 *   <li>v8 package has no V12 storage SDK imports and no v12 cross-package references</li>
 *   <li>v12 package has no V8 SDK imports and no v8 cross-package references</li>
 *   <li>Parent blobstorage package has no Azure SDK storage imports</li>
 * </ul>
 *
 * Exception: {@code com.azure.identity.*} and {@code com.azure.core.credential.*} are
 * allowed in v8 for AAD service principal authentication (see OAK-12164 plan section 2.4).
 */
public class AzureIsolationTest {

    // V12 storage SDK pattern (forbidden in v8 and parent)
    private static final Pattern V12_STORAGE_SDK = Pattern.compile(
            "import\\s+com\\.azure\\.storage\\.");

    // V8 SDK pattern (forbidden in v12 and parent)
    private static final Pattern V8_SDK = Pattern.compile(
            "import\\s+com\\.microsoft\\.azure\\.");

    // Cross-package references
    private static final Pattern V12_PACKAGE_REF = Pattern.compile(
            "blobstorage\\.v12\\.");

    private static final Pattern V8_PACKAGE_REF = Pattern.compile(
            "blobstorage\\.v8\\.");

    private static final Path SRC_ROOT = findSourceRoot("src/main/java");
    private static final Path TEST_ROOT = findSourceRoot("src/test/java");

    private static final String BLOBSTORAGE_PKG = "org/apache/jackrabbit/oak/blob/cloud/azure/blobstorage";

    @Test
    public void v8SourceMustNotImportV12StorageSDK() throws IOException {
        List<String> violations = new ArrayList<>();
        scanPackage(SRC_ROOT, "v8", V12_STORAGE_SDK, "com.azure.storage.* import", violations);
        scanPackage(TEST_ROOT, "v8", V12_STORAGE_SDK, "com.azure.storage.* import", violations);
        assertNoViolations("v8 must not import V12 storage SDK", violations);
    }

    @Test
    public void v8SourceMustNotReferenceV12Package() throws IOException {
        List<String> violations = new ArrayList<>();
        scanPackage(SRC_ROOT, "v8", V12_PACKAGE_REF, "blobstorage.v12.* reference", violations);
        scanPackage(TEST_ROOT, "v8", V12_PACKAGE_REF, "blobstorage.v12.* reference", violations);
        assertNoViolations("v8 must not reference v12 package", violations);
    }

    @Test
    public void v12SourceMustNotImportV8SDK() throws IOException {
        List<String> violations = new ArrayList<>();
        scanPackage(SRC_ROOT, "v12", V8_SDK, "com.microsoft.azure.* import", violations);
        scanPackage(TEST_ROOT, "v12", V8_SDK, "com.microsoft.azure.* import", violations);
        assertNoViolations("v12 must not import V8 SDK", violations);
    }

    @Test
    public void v12SourceMustNotReferenceV8Package() throws IOException {
        List<String> violations = new ArrayList<>();
        scanPackage(SRC_ROOT, "v12", V8_PACKAGE_REF, "blobstorage.v8.* reference", violations);
        scanPackage(TEST_ROOT, "v12", V8_PACKAGE_REF, "blobstorage.v8.* reference", violations);
        assertNoViolations("v12 must not reference v8 package", violations);
    }

    @Test
    public void parentSourcePackageMustNotImportAzureStorageSDK() throws IOException {
        // Only check src/main/java — test infrastructure (AzuriteDockerRule, etc.)
        // legitimately needs both SDK types to create containers for both backends.
        List<String> violations = new ArrayList<>();
        scanParentPackage(SRC_ROOT, V12_STORAGE_SDK, "com.azure.storage.* import", violations);
        scanParentPackage(SRC_ROOT, V8_SDK, "com.microsoft.azure.* import", violations);
        assertNoViolations("parent blobstorage source package must not import Azure SDK storage types", violations);
    }

    /**
     * Scan Java files in a versioned subpackage (v8/ or v12/) for forbidden patterns.
     */
    private void scanPackage(Path sourceRoot, String subpackage, Pattern forbidden,
                             String description, List<String> violations) throws IOException {
        Path pkgDir = sourceRoot.resolve(BLOBSTORAGE_PKG).resolve(subpackage);
        if (!Files.isDirectory(pkgDir)) {
            return;
        }
        Files.walkFileTree(pkgDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(".java")) {
                    checkFile(file, forbidden, description, violations);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Scan Java files in the parent blobstorage package ONLY (not subpackages).
     */
    private void scanParentPackage(Path sourceRoot, Pattern forbidden,
                                   String description, List<String> violations) throws IOException {
        Path pkgDir = sourceRoot.resolve(BLOBSTORAGE_PKG);
        if (!Files.isDirectory(pkgDir)) {
            return;
        }
        // Only direct children, not v8/ or v12/ subdirectories
        Files.list(pkgDir)
                .filter(p -> p.toString().endsWith(".java"))
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        checkFile(file, forbidden, description, violations);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void checkFile(Path file, Pattern forbidden, String description,
                           List<String> violations) throws IOException {
        List<String> lines = Files.readAllLines(file);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (forbidden.matcher(line).find()) {
                // Check allowlist: com.azure.identity.* and com.azure.core.credential.* in v8 for AAD
                if (isAllowedV8AadImport(file, line)) {
                    continue;
                }
                violations.add(String.format("%s:%d — %s: %s",
                        file.getFileName(), i + 1, description, line.trim()));
            }
        }
    }

    /**
     * com.azure.identity.* and com.azure.core.credential.* are allowed in v8
     * for AAD service principal authentication (the Azure Identity library is
     * SDK-version-neutral).
     */
    private boolean isAllowedV8AadImport(Path file, String line) {
        if (!file.toString().contains("/v8/")) {
            return false;
        }
        return line.contains("com.azure.identity.") || line.contains("com.azure.core.credential.");
    }

    private void assertNoViolations(String context, List<String> violations) {
        assertTrue(context + ":\n" + String.join("\n", violations), violations.isEmpty());
    }

    private static Path findSourceRoot(String relativePath) {
        // Walk up from this class's location to find the module root
        Path moduleRoot = Paths.get("oak-blob-cloud-azure");
        if (!Files.isDirectory(moduleRoot)) {
            // Try from current directory (might already be in module)
            moduleRoot = Paths.get(".");
        }
        return moduleRoot.resolve(relativePath);
    }
}
