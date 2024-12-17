/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jackrabbit.guava.common.collect.Sets.union;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.append;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.copy;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.lexComparator;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.lineBreakAwareComparator;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.merge;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.readStringsAsSet;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.sort;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.unescapeLineBreaks;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Splitter;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.sort.EscapeUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link FileIOUtils}
 */
public class FileIOUtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("./target"));

    private static final Random RANDOM = new Random();

    @Test
    public void writeReadStrings() throws Exception {
        Set<String> added = Set.of("a", "z", "e", "b");
        File f = assertWrite(added.iterator(), false, added.size());

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), false);

        assertEquals(added, retrieved);
    }

    @Test
    public void writeCustomReadOrgStrings() throws Exception {
        Set<String> added = Set.of("a-", "z-", "e-", "b-");
        Set<String> actual = Set.of("a", "z", "e", "b");

        File f = folder.newFile();
        int count = writeStrings(added.iterator(), f, false, new java.util.function.Function<String, String>() {
            @Nullable @Override public String apply(@Nullable String input) {
                return Splitter.on("-").trimResults().omitEmptyStrings().splitToList(input).get(0);
            }
        }, null, null);
        assertEquals(added.size(), count);

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), false);
        assertEquals(actual, retrieved);
    }

    @Test
    public void writeReadStringsWithLineBreaks() throws IOException {
        Set<String> added = new HashSet<>(getLineBreakStrings());
        File f = assertWrite(added.iterator(), true, added.size());

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), true);

        assertEquals(added, retrieved);
    }

    @Test
    public void writeReadRandomStrings() throws Exception {
        Set<String> added = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            added.add(getRandomTestString());
        }
        File f = assertWrite(added.iterator(), true, added.size());

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), true);

        assertEquals(added, retrieved);
    }

    @Test
    public void compareWithLineBreaks() throws Exception {
        Comparator<String> cmp = lineBreakAwareComparator(lexComparator);
        List<String> strs = getLineBreakStrings();
        Collections.sort(strs);

        // Escape line breaks and then compare with string sorted
        List<String> escapedStrs = escape(getLineBreakStrings());
        Collections.sort(escapedStrs, cmp);

        assertEquals(strs, unescape(escapedStrs));
    }

    @Test
    public void sortTest() throws IOException {
        List<String> list = new ArrayList<>(Arrays.asList("a", "z", "e", "b"));
        File f = assertWrite(list.iterator(), false, list.size());

        sort(f);

        BufferedReader reader =
            new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8));
        String line;
        List<String> retrieved = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            retrieved.add(line);
        }
        closeQuietly(reader);
        Collections.sort(list);
        assertArrayEquals(Arrays.toString(list.toArray()), list.toArray(), retrieved.toArray());
    }

    @Test
    public void sortCustomComparatorTest() throws IOException {
        List<String> list = getLineBreakStrings();
        File f = assertWrite(list.iterator(), true, list.size());

        sort(f, lineBreakAwareComparator(lexComparator));

        BufferedReader reader =
            new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8));
        String line;
        List<String> retrieved = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            retrieved.add(unescapeLineBreaks(line));
        }
        closeQuietly(reader);
        Collections.sort(list);
        assertArrayEquals(Arrays.toString(list.toArray()), list.toArray(), retrieved.toArray());
    }
    
    @Test
    public void sortLargeFileWithCustomComparatorTest() throws IOException {
        final int numEntries = 100000;      // must be large enough to trigger split/merge functionality of the sort
        long[] entries = new long[numEntries];
        Random r = new Random(0);
        for (int i = 0; i < numEntries; i++) {
            entries[i] = r.nextLong();
        }

        Iterator<String> hexEntries = Arrays.stream(entries).
                mapToObj(input -> Long.toHexString(input)).iterator();
        File f = assertWrite(hexEntries, false, numEntries);

        Comparator<String> prefixComparator = new Comparator<String>() {
            @Override public int compare(String s1, String s2) {
                return s1.substring(0, 3).compareTo(s2.substring(0, 3));
            }
        };

        sort(f, prefixComparator);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8));
        String previous = reader.readLine().substring(0, 3);
        while (true) {
            String current = reader.readLine();
            if (current == null) {
                break;
            }
            current = current.substring(0, 3);
            assertFalse("Distinct sort didn't filter out duplicates properly.", previous.equals(current));
            assertTrue("Sort didn't create increasing order", previous.compareTo(current) < 0);
            previous = current;
        }
        closeQuietly(reader);
    }

    @Test
    public void testCopy() throws IOException{
        File f = copy(randomStream(0, 256));
        assertTrue("File does not exist", f.exists());
        Assert.assertEquals("File length not equal to byte array from which copied",
            256, f.length());
        assertTrue("Could not delete file", f.delete());
    }

    @Test
    public void appendTest() throws IOException {
        Set<String> added1 = Set.of("a", "z", "e", "b");
        File f1 = assertWrite(added1.iterator(), false, added1.size());

        Set<String> added2 = Set.of("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = Set.of("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        append(List.of(f2, f3), f1, true);
        assertEquals(union(union(added1, added2), added3),
            readStringsAsSet(new FileInputStream(f1), false));
        assertTrue(!f2.exists());
        assertTrue(!f3.exists());
        assertTrue(f1.exists());
    }

    @Test
    public void appendTestNoDelete() throws IOException {
        Set<String> added1 = Set.of("a", "z", "e", "b");
        File f1 = assertWrite(added1.iterator(), false, added1.size());

        Set<String> added2 = Set.of("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = Set.of("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        append(List.of(f2, f3), f1, false);

        assertEquals(union(union(added1, added2), added3),
            readStringsAsSet(new FileInputStream(f1), false));
        assertTrue(f2.exists());
        assertTrue(f3.exists());
        assertTrue(f1.exists());
    }

    @Test
    public void appendTestFileDeleteOnError() throws IOException {
        Set<String> added2 = Set.of("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = Set.of("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        try {
            append(List.of(f2, f3), null, true);
        } catch (Exception e) {
        }
        assertTrue(!f2.exists());
        assertTrue(!f3.exists());
    }

    @Test
    public void appendRandomizedTest() throws Exception {
        Set<String> added1 = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            added1.add(getRandomTestString());
        }
        File f1 = assertWrite(added1.iterator(), true, added1.size());

        Set<String> added2 = Set.of("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), true, added2.size());

        append(List.of(f2), f1, true);

        assertEquals(union(added1, added2),
            readStringsAsSet(new FileInputStream(f1), true));
    }

    @Test
    public void appendWithLineBreaksTest() throws IOException {
        Set<String> added1 = new HashSet<>(getLineBreakStrings());
        File f1 = assertWrite(added1.iterator(), true, added1.size());

        Set<String> added2 = Set.of("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), true, added2.size());

        append(List.of(f1), f2, true);

        assertEquals(union(added1, added2), readStringsAsSet(new FileInputStream(f2), true));
    }

    @Test
    public void mergeWithErrorsTest() throws IOException {
        Set<String> added2 = Set.of("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = Set.of("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        try {
            merge(List.of(f2, f3), null);
        } catch(Exception e) {}

        assertTrue(!f2.exists());
        assertTrue(!f3.exists());
    }

    @Test
    public void fileIteratorTest() throws Exception {
        Set<String> added = Set.of("a", "z", "e", "b");
        File f = assertWrite(added.iterator(), false, added.size());

        org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator<String> iterator =
                org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator.wrap(FileUtils.lineIterator(f, UTF_8.toString()));

        assertEquals(added, CollectionUtils.toSet(iterator));
        assertTrue(f.exists());
    }

    @Test
    public void fileIteratorBurnTest() throws Exception {
        Set<String> added = Set.of("a", "z", "e", "b");
        File f = assertWrite(added.iterator(), false, added.size());

        org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator<String> iterator =
                org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator.wrap(FileUtils.lineIterator(f, UTF_8.toString()), f);

        assertEquals(added, CollectionUtils.toSet(iterator));
        assertTrue(!f.exists());
    }

    @Test
    public void fileIteratorLineBreakTest() throws IOException {
        Set<String> added = new HashSet<>(getLineBreakStrings());
        File f = assertWrite(added.iterator(), true, added.size());

        org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator<String> iterator = new org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator<String>(
                FileUtils.lineIterator(f, UTF_8.toString()), f, EscapeUtils::unescapeLineBreaks);

        assertEquals(added, CollectionUtils.toSet(iterator));
        assertTrue(!f.exists());
    }

    @Test
    public void fileIteratorRandomizedTest() throws Exception {
        Set<String> added = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            added.add(getRandomTestString());
        }
        File f = assertWrite(added.iterator(), true, added.size());

        org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator<String> iterator = new org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator<String>(
                FileUtils.lineIterator(f, UTF_8.toString()), f, EscapeUtils::unescapeLineBreaks);

        assertEquals(added, CollectionUtils.toSet(iterator));
        assertTrue(!f.exists());
    }

    @Test
    public void copyStreamToFile() throws Exception {
        Set<String> added = Set.of("a", "z", "e", "b");
        File f = assertWrite(added.iterator(), false, added.size());

        File f2 = folder.newFile();
        FileIOUtils.copyInputStreamToFile(new FileInputStream(f), f2);
        assertEquals(added, readStringsAsSet(new FileInputStream(f), false));
        assertTrue(f.exists());
    }

    @Test
    public void copyStreamToFileNoPartialCreation() throws Exception {
        File f = folder.newFile();
        FileIOUtils.copyInputStreamToFile(randomStream(12, 8192), f);

        File f2 = folder.newFile();
        try {
            FileIOUtils.copyInputStreamToFile(new ErrorInputStream(f, 4096), f2);
            Assert.fail("Should have failed with IOException");
        } catch (Exception e) {}

        assertTrue(f.exists());
        assertTrue(!f2.exists());
    }

    @Test
    public void testTransformingComparator() throws Exception {
        Comparator<String> delegate = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        };
        java.util.function.Function<String, String> fun = x -> x.toLowerCase(Locale.ENGLISH);
        FileIOUtils.TransformingComparator comp = new FileIOUtils.TransformingComparator(delegate, fun);

        assertTrue(0 > delegate.compare("A", "a"));
        assertTrue(0 < delegate.compare("a", "A"));
        assertTrue(0 == comp.compare("a", "A"));
    }

    private static List<String> getLineBreakStrings() {
        return new ArrayList<>(Arrays.asList("ab\nc\r", "ab\\z", "a\\\\z\nc",
            "/a", "/a/b\nc", "/a/b\rd", "/a/b\r\ne", "/a/c"));
    }

    private static List<String> escape(List<String> list) {
        List<String> escaped = new ArrayList<>();
        for (String s : list) {
            escaped.add(escapeLineBreak(s));
        }
        return escaped;
    }

    private static List<String> unescape(List<String> list) {
        List<String> unescaped = new ArrayList<>();
        for (String s : list) {
            unescaped.add(unescapeLineBreaks(s));
        }
        return unescaped;
    }

    private File assertWrite(Iterator<String> iterator, boolean escape, int size)
        throws IOException {
        File f = folder.newFile();
        int count = writeStrings(iterator, f, escape);
        assertEquals(size, count);
        return f;
    }

    private static String getRandomTestString() throws Exception {
        boolean valid = false;
        StringBuilder buffer = new StringBuilder();
        while(!valid) {
            int length = RANDOM.nextInt(40);
            for (int i = 0; i < length; i++) {
                buffer.append((char) (RANDOM.nextInt(Character.MAX_VALUE)));
            }
            String s = buffer.toString();
            CharsetEncoder encoder = Charset.forName(UTF_8.toString()).newEncoder();
            try {
                encoder.encode(CharBuffer.wrap(s));
                valid = true;
            } catch (CharacterCodingException e) {
                buffer = new StringBuilder();
            }
        }
        return buffer.toString();
    }

    private static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    /**
     * Throws error after reading partially defined by max
     */
    private static class ErrorInputStream extends FileInputStream {
        private long bytesread;
        private long max;

        ErrorInputStream(File file, long max) throws FileNotFoundException {
            super(file);
            this.max = max;
        }

        @Override
        public int read(byte b[]) throws IOException {
            bytesread += b.length;
            if (bytesread > max) {
                throw new IOException("Disconnected");
            }
            return super.read(b);
        }
    }
}
