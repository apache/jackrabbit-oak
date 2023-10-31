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
package org.apache.jackrabbit.oak.commons.sort;

import org.apache.jackrabbit.oak.commons.Compression;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertArrayEquals;

public class ExternalSortByteArrayTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));
    private final static Charset charset = StandardCharsets.UTF_8;
    private final static Comparator<BinaryTestLine> cmp = Comparator.naturalOrder();
    private final static Function<byte[], BinaryTestLine> byteArrayToType = line -> line != null ? new BinaryTestLine(line) : null;
    private final static Function<BinaryTestLine, byte[]> typeToByteArray = tl -> tl != null ? tl.bytes : null;

    @Test
    public void sortManyFilesNoCompression() throws Exception {
        sortManyFiles(Compression.NONE);
    }

    @Test
    public void sortManyFilesGzipCompression() throws Exception {
        sortManyFiles(Compression.GZIP);
    }

    @Test
    public void sortManyFilesLZ4Compression() throws Exception {
        sortManyFiles(ExternalSortTest.LZ4());
    }

    public void sortManyFiles(Compression compression) throws Exception {
        int testCount = 1000;
        List<BinaryTestLine> testLines = generateTestLines(testCount);

        List<BinaryTestLine> testLinesShuffled = new ArrayList<>(testLines);
        Collections.shuffle(testLinesShuffled);

        List<Path> intermediateFiles = createIntermediateFiles(testLinesShuffled, 10, compression);
        Path resultFile = folder.newFile(compression.addSuffix("sorted.json")).toPath();

        try (BufferedOutputStream bos = new BufferedOutputStream(compression.getOutputStream(Files.newOutputStream(resultFile)))) {
            ExternalSortByteArray.mergeSortedFilesBinary(intermediateFiles,
                    bos,
                    cmp,
                    true,
                    compression,
                    typeToByteArray,
                    byteArrayToType);
        }

        ArrayList<String> lines = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(compression.getInputStream(Files.newInputStream(resultFile)), charset))) {
            while (true) {
                String line = bufferedReader.readLine();
                if (line == null) {
                    break;
                }
                lines.add(line);
            }
        }
        String[] actual = lines.toArray(new String[0]);
        String[] expected = testLines.stream().map(tl -> tl.line).toArray(String[]::new);
        assertArrayEquals(expected, actual);
    }

    private List<BinaryTestLine> generateTestLines(int numberOfLines) {
        List<BinaryTestLine> testLines = new ArrayList<>(numberOfLines);
        for (int i = 0; i < numberOfLines; i++) {
            testLines.add(new BinaryTestLine(i + ":" + "foo-" + i));
        }
        return testLines;
    }

    private List<Path> createIntermediateFiles(List<BinaryTestLine> ffsLines, int numberOfFiles, Compression compression) throws Exception {
        Iterator<BinaryTestLine> ffsIter = ffsLines.iterator();
        Path workFolder = folder.newFolder("merge_many_test").toPath();
        ArrayList<Path> intermediateFiles = new ArrayList<>(numberOfFiles);
        int linesPerFile = ffsLines.size() / numberOfFiles;

        for (int fileIdx = 0; fileIdx < numberOfFiles; fileIdx++) {
            Path intermediateFile = workFolder.resolve(compression.addSuffix("intermediate-" + fileIdx + ".json"));
            ArrayList<BinaryTestLine> binaryTestLinesInFile = new ArrayList<>();
            while (binaryTestLinesInFile.size() < linesPerFile && ffsIter.hasNext()) {
                binaryTestLinesInFile.add(ffsIter.next());
            }
            if (fileIdx == numberOfFiles - 1) {
                // Add the remaining elements to the last file
                while (ffsIter.hasNext()) {
                    binaryTestLinesInFile.add(ffsIter.next());
                }
            }
            binaryTestLinesInFile.sort(cmp);

            try (BufferedWriter bw = new BufferedWriter(
                    new OutputStreamWriter(
                            compression.getOutputStream(
                                    Files.newOutputStream(intermediateFile))))) {
                for (BinaryTestLine binaryTestLine : binaryTestLinesInFile) {
                    bw.write(binaryTestLine.line);
                    bw.write("\n");
                }
            }
            intermediateFiles.add(intermediateFile);
        }
        return intermediateFiles;
    }

    private static class BinaryTestLine implements Comparable<BinaryTestLine> {
        final String line;
        final int value;
        final byte[] bytes;

        public BinaryTestLine(String line) {
            this.line = line;
            this.value = Integer.parseInt(line.substring(0, line.indexOf(':')));
            this.bytes = line.getBytes(StandardCharsets.UTF_8);
        }

        public BinaryTestLine(byte[] bytes) {
            this.bytes = bytes;
            this.line = new String(bytes, StandardCharsets.UTF_8);
            this.value = Integer.parseInt(line.substring(0, line.indexOf(':')));
        }

        @Override
        public int compareTo(BinaryTestLine o) {
            return Integer.compare(value, o.value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BinaryTestLine binaryTestLine = (BinaryTestLine) o;
            return line.equals(binaryTestLine.line);
        }

        @Override
        public int hashCode() {
            return line.hashCode();
        }

        @Override
        public String toString() {
            return line;
        }
    }
}