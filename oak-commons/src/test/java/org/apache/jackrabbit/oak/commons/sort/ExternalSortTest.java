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

import static com.google.common.collect.Iterables.transform;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;
import java.util.function.Function;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for simple App.
 * 
 * Source copied from a publicly available library.
 * 
 * @see <a
 *      href="https://code.google.com/p/externalsortinginjava/">https://code.google.com/p/externalsortinginjava</a>
 * 
 *      Goal: offer a generic external-memory sorting program in Java.
 * 
 *      It must be : - hackable (easy to adapt) - scalable to large files -
 *      sensibly efficient.
 * 
 *      This software is in the public domain.
 * 
 *      Usage: java org/apache/oak/commons/sort//ExternalSort somefile.txt
 *      out.txt
 * 
 *      You can change the default maximal number of temporary files with the -t
 *      flag: java org/apache/oak/commons/sort/ExternalSort somefile.txt out.txt
 *      -t 3
 * 
 *      You can change the default maximum memory available with the -m flag:
 *      java org/apache/oak/commons/sort/ExternalSort somefile.txt out.txt -m
 *      8192
 * 
 *      For very large files, you might want to use an appropriate flag to
 *      allocate more memory to the Java VM: java -Xms2G
 *      org/apache/oak/commons/sort/ExternalSort somefile.txt out.txt
 * 
 *      By (in alphabetical order) Philippe Beaudoin, Eleftherios Chetzakis, Jon
 *      Elsas, Christan Grant, Daniel Haran, Daniel Lemire, Sugumaran
 *      Harikrishnan, Jerry Yang, First published: April 2010 originally posted
 *      at
 *      http://lemire.me/blog/archives/2010/04/01/external-memory-sorting-in-java
 */
public class ExternalSortTest {
    private static final String TEST_FILE1_TXT = "test-file-1.txt";
    private static final String TEST_FILE2_TXT = "test-file-2.txt";
    private static final String TEST_FILE1_CSV = "test-file-1.csv";
    private static final String TEST_FILE2_CSV = "test-file-2.csv";

    private static final String[] EXPECTED_SORT_RESULTS = { "a", "b", "b", "e",
            "f", "i", "m", "o", "u", "u", "x", "y", "z" };
    private static final String[] EXPECTED_MERGE_RESULTS = { "a", "a", "b",
            "c", "c", "d", "e", "e", "f", "g", "g", "h", "i", "j", "k" };
    private static final String[] EXPECTED_MERGE_DISTINCT_RESULTS = { "a", "b",
            "c", "d", "e", "f", "g", "h", "i", "j", "k" };
    private static final String[] EXPECTED_HEADER_RESULTS = { "HEADER, HEADER",
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k" };
    private static final String[] EXPECTED_DISTINCT_RESULTS = { "a", "b", "e",
            "f", "i", "m", "o", "u", "x", "y", "z" };
    private static final String[] SAMPLE = { "f", "m", "b", "e", "i", "o", "u",
            "x", "a", "y", "z", "b", "u" };
    private static final String[] EXPECTED_CSV_DISTINCT_RESULTS = { "a,1", "b,2a", "e,3", "f,4", "i,5", "m,6", "o,7", 
                                                                      "u,8a", "x,9", "y,10", "z,11"};
    private static final String[] EXPECTED_CSV_RESULTS = { "a,1", "b,2a", "b,2b", "e,3", "f,4", "i,5", "m,6", "o,7",
                                                             "u,8a", "u,8b", "x,9", "y,10", "z,11"};
    private File file1;
    private File file2;
    private File csvFile;
    private File csvFile2;
    private List<File> fileList;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /**
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        this.fileList = new ArrayList<File>(3);
        this.file1 = new File(this.getClass().
                getResource(TEST_FILE1_TXT).toURI());
        this.file2 = new File(this.getClass().
                getResource(TEST_FILE2_TXT).toURI());
        this.csvFile = new File(this.getClass().
                getResource(TEST_FILE1_CSV).toURI());
        this.csvFile2 = new File(this.getClass().
                 getResource(TEST_FILE2_CSV).toURI());

        File tmpFile1 = new File(this.file1.getPath().toString() + ".tmp");
        File tmpFile2 = new File(this.file2.getPath().toString() + ".tmp");

        copyFile(this.file1, tmpFile1);
        copyFile(this.file2, tmpFile2);

        this.fileList.add(tmpFile1);
        this.fileList.add(tmpFile2);
    }

    /**
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        this.file1 = null;
        this.file2 = null;
        this.csvFile = null;
        for (File f : this.fileList) {
            f.delete();
        }
        this.fileList.clear();
        this.fileList = null;
    }

    private static void copyFile(File sourceFile, File destFile)
            throws IOException {
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel source = null;
        FileChannel destination = null;

        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
        } finally {
            if (source != null) {
                source.close();
            }
            if (destination != null) {
                destination.close();
            }
        }
    }

    @Test
    public void testEmptyFiles() throws Exception {
        File f1 = folder.newFile();
        File f2 = folder.newFile();
        ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(f1), f2);
        if (f2.length() != 0) {
            throw new RuntimeException("empty files should end up emtpy");
        }
    }

    @Test
    public void testMergeSortedFiles() throws Exception {
        String line;
        List<String> result;
        BufferedReader bf;
        Comparator<String> cmp = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };
        File out = folder.newFile();
        ExternalSort.mergeSortedFiles(this.fileList, out, cmp,
                Charset.defaultCharset(), false);

        bf = new BufferedReader(new FileReader(out));

        result = new ArrayList<String>();
        while ((line = bf.readLine()) != null) {
            result.add(line);
        }
        bf.close();
        assertArrayEquals(Arrays.toString(result.toArray()),
                EXPECTED_MERGE_RESULTS, result.toArray());
    }

    @Test
    public void testMergeSortedFilesDistinct() throws Exception {
        String line;
        List<String> result;
        BufferedReader bf;
        Comparator<String> cmp = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };
        File out = folder.newFile();
        ExternalSort.mergeSortedFiles(this.fileList, out, cmp,
                Charset.defaultCharset(), true);

        bf = new BufferedReader(new FileReader(out));

        result = new ArrayList<String>();
        while ((line = bf.readLine()) != null) {
            result.add(line);
        }
        bf.close();
        assertArrayEquals(Arrays.toString(result.toArray()),
                EXPECTED_MERGE_DISTINCT_RESULTS, result.toArray());
    }

    @Test
    public void testMergeSortedFilesAppend() throws Exception {
        String line;
        List<String> result;
        BufferedReader bf;
        Comparator<String> cmp = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        File out = folder.newFile();
        writeStringToFile(out, "HEADER, HEADER\n");

        ExternalSort.mergeSortedFiles(this.fileList, out, cmp,
                Charset.defaultCharset(), true, true, false);

        bf = new BufferedReader(new FileReader(out));

        result = new ArrayList<String>();
        while ((line = bf.readLine()) != null) {
            result.add(line);
        }
        bf.close();
        assertArrayEquals(Arrays.toString(result.toArray()),
                EXPECTED_HEADER_RESULTS, result.toArray());
    }

    @Test
    public void testSortAndSave() throws Exception {
        File f;
        String line;
        List<String> result;
        BufferedReader bf;

        List<String> sample = Arrays.asList(SAMPLE);
        Comparator<String> cmp = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };
        f = ExternalSort.sortAndSave(sample, cmp, Charset.defaultCharset(),
                null, false, false);
        assertNotNull(f);
        assertTrue(f.exists());
        assertTrue(f.length() > 0);
        bf = new BufferedReader(new FileReader(f));

        result = new ArrayList<String>();
        while ((line = bf.readLine()) != null) {
            result.add(line);
        }
        bf.close();
        assertArrayEquals(Arrays.toString(result.toArray()),
                EXPECTED_SORT_RESULTS, result.toArray());
    }

    @Test
    public void testSortAndSaveDistinct() throws Exception {
        File f;
        String line;
        List<String> result;
        BufferedReader bf;
        List<String> sample = Arrays.asList(SAMPLE);
        Comparator<String> cmp = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        f = ExternalSort.sortAndSave(sample, cmp, Charset.defaultCharset(),
                null, true, false);
        assertNotNull(f);
        assertTrue(f.exists());
        assertTrue(f.length() > 0);
        bf = new BufferedReader(new FileReader(f));

        result = new ArrayList<String>();
        while ((line = bf.readLine()) != null) {
            result.add(line);
        }
        bf.close();
        assertArrayEquals(Arrays.toString(result.toArray()),
                EXPECTED_DISTINCT_RESULTS, result.toArray());
    }

    @Test
    public void testSortInBatch() throws Exception {
        Comparator<String> cmp = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        List<File> listOfFiles = ExternalSort.sortInBatch(this.csvFile, cmp,
                ExternalSort.DEFAULTMAXTEMPFILES, ExternalSort.DEFAULT_MAX_MEM_BYTES, 
                Charset.defaultCharset(),
                null, false, 1, false);
        assertEquals(1, listOfFiles.size());

        ArrayList<String> result = readLines(listOfFiles.get(0));
        assertArrayEquals(Arrays.toString(result.toArray()),
                EXPECTED_MERGE_DISTINCT_RESULTS, result.toArray());
    }

    /**
     * Sample case to sort csv file.
     * 
     * @throws Exception
     * 
     */
    @Test
    public void testCSVSorting() throws Exception {
        testCSVSortingWithParams(false);
        testCSVSortingWithParams(true);
    }

    @Test
    public void testCSVKeyValueSorting() throws Exception {
        testCSVSortKeyValue(false);
        testCSVSortKeyValue(true);
    }
    
    /**
     * Sample case to sort csv file with key, value pair.
     *
     * @param distinct if distinct records need to be omitted
     * @throws Exception
     *
     */    
    public void testCSVSortKeyValue(boolean distinct) throws Exception {
        
        File out = folder.newFile();
        
        Comparator<String> cmp =   new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.split(",")[0].compareTo(s2.split(",")[0]);
            }
        };
        
        List<File> listOfFiles = ExternalSort.sortInBatch(this.csvFile2, cmp,
                                                             ExternalSort.DEFAULTMAXTEMPFILES,
                                                             ExternalSort.DEFAULT_MAX_MEM_BYTES,
                                                             Charset.defaultCharset(),
                                                             null, distinct, 0, false);
        
        // now merge with append
        ExternalSort.mergeSortedFiles(listOfFiles, out, cmp,
                                         Charset.defaultCharset(), distinct, true, false);
        ArrayList<String> result = readLines(out);
        
        if (distinct) {
            assertEquals(11, result.size());
            assertArrayEquals(Arrays.toString(result.toArray()), EXPECTED_CSV_DISTINCT_RESULTS, result.toArray());
        } else {
            assertEquals(13, result.size());
            assertArrayEquals(Arrays.toString(result.toArray()), EXPECTED_CSV_RESULTS, result.toArray());            
        }
        
    }
    
    /**
     * Sample case to sort csv file.
     * 
     * @param usegzip use compression for temporary files
     * @throws Exception
     * 
     */
    public void testCSVSortingWithParams(boolean usegzip) throws Exception {

        File out = folder.newFile();

        Comparator<String> cmp = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };

        // read header
        FileReader fr = new FileReader(this.csvFile);
        Scanner scan = new Scanner(fr);
        String head = scan.nextLine();

        // write to the file
        writeStringToFile(out, head + "\n");

        // omit the first line, which is the header..
        List<File> listOfFiles = ExternalSort.sortInBatch(this.csvFile, cmp,
                ExternalSort.DEFAULTMAXTEMPFILES, 
                ExternalSort.DEFAULT_MAX_MEM_BYTES,
                Charset.defaultCharset(),
                null, false, 1, usegzip);

        // now merge with append
        ExternalSort.mergeSortedFiles(listOfFiles, out, cmp,
                Charset.defaultCharset(), false, true, usegzip);

        ArrayList<String> result = readLines(out);

        assertEquals(12, result.size());
        assertArrayEquals(Arrays.toString(result.toArray()),
                EXPECTED_HEADER_RESULTS, result.toArray());

    }

    @Test
    public void customType() throws Exception{
        File out = folder.newFile();
        int testCount = 1000;

        List<TestLine> testLines = new ArrayList<>(1000);
        for (int i = 0; i < testCount; i++) {
            testLines.add(new TestLine(i + ":" + "foo-"+i));
        }

        Collections.shuffle(testLines);

        Comparator<TestLine> cmp = Comparator.naturalOrder();
        Charset charset = Charsets.UTF_8;

        Function<String, TestLine> stringToType = line -> line != null ? new TestLine(line) : null;
        Function<TestLine, String> typeToString = tl -> tl != null ? tl.line : null;

        String testData = Joiner.on('\n').join(transform(testLines, tl -> tl.line));
        File testFile = folder.newFile();
        Files.write(testData, testFile, charset);

        List<File> listOfFiles = ExternalSort.sortInBatch(testFile, cmp,
                ExternalSort.DEFAULTMAXTEMPFILES,
                100,
                charset,
                folder.newFolder(),
                false,
                0,
                false,
                typeToString,
                stringToType);

        ExternalSort.mergeSortedFiles(listOfFiles, out, cmp,
                charset, false, true, false, typeToString, stringToType);


        Collections.sort(testLines);

        List<TestLine> linesFromSortedFile = new ArrayList<>();
        Files.readLines(out, charset).forEach(line -> linesFromSortedFile.add(new TestLine(line)));

        assertEquals(testLines, linesFromSortedFile);

    }

    private static class TestLine implements Comparable<TestLine> {
        final String line;
        final int value;

        public TestLine(String line) {
            this.line = line;
            this.value = Integer.valueOf(line.substring(0, line.indexOf(':')));
        }

        @Override
        public int compareTo(TestLine o) {
            return Ints.compare(value, o.value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestLine testLine = (TestLine) o;

            if (value != testLine.value) return false;
            return line.equals(testLine.line);
        }

        @Override
        public int hashCode() {
            int result = line.hashCode();
            result = 31 * result + value;
            return result;
        }

        @Override
        public String toString() {
            return line;
        }
    }

    public static ArrayList<String> readLines(File f) throws IOException {
        ArrayList<String> answer = new ArrayList<String>();
        BufferedReader r = new BufferedReader(new FileReader(f));
        try {
            String line;
            while ((line = r.readLine()) != null) {
                answer.add(line);
            }
        } finally {
            r.close();
        }
        return answer;
    }

    public static void writeStringToFile(File f, String s) throws IOException {
        FileOutputStream out = new FileOutputStream(f);
        try {
            out.write(s.getBytes());
        } finally {
            out.close();
        }
    }

}
