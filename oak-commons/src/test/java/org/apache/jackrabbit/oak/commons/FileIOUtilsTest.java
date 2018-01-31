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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.FileIOUtils.BurnOnCloseFileIterator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


/**
 * Tests for {@link FileIOUtils}
 */
public class FileIOUtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("./target"));

    private static final Random RANDOM = new Random();

    @Test
    public void writeReadStrings() throws Exception {
        Set<String> added = newHashSet("a", "z", "e", "b");
        File f = assertWrite(added.iterator(), false, added.size());

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), false);

        assertEquals(added, retrieved);
    }

    @Test
    public void writeCustomReadOrgStrings() throws Exception {
        Set<String> added = newHashSet("a-", "z-", "e-", "b-");
        Set<String> actual = newHashSet("a", "z", "e", "b");

        File f = folder.newFile();
        int count = writeStrings(added.iterator(), f, false, new Function<String, String>() {
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
        Set<String> added = newHashSet(getLineBreakStrings());
        File f = assertWrite(added.iterator(), true, added.size());

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), true);

        assertEquals(added, retrieved);
    }

    @Test
    public void writeReadRandomStrings() throws Exception {
        Set<String> added = newHashSet();
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
        List<String> list = newArrayList("a", "z", "e", "b");
        File f = assertWrite(list.iterator(), false, list.size());

        sort(f);

        BufferedReader reader =
            new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8));
        String line;
        List<String> retrieved = newArrayList();
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
        List<String> retrieved = newArrayList();
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
        
        Iterator<Long> boxedEntries = Longs.asList(entries).iterator();
        Iterator<String> hexEntries = Iterators.transform(boxedEntries, new Function<Long, String>() {
                    @Nullable @Override public String apply(@Nullable Long input) {
                        return Long.toHexString(input);
                    }
                });
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
        Set<String> added1 = newHashSet("a", "z", "e", "b");
        File f1 = assertWrite(added1.iterator(), false, added1.size());

        Set<String> added2 = newHashSet("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = newHashSet("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        append(newArrayList(f2, f3), f1, true);
        assertEquals(union(union(added1, added2), added3),
            readStringsAsSet(new FileInputStream(f1), false));
        assertTrue(!f2.exists());
        assertTrue(!f3.exists());
        assertTrue(f1.exists());
    }

    @Test
    public void appendTestNoDelete() throws IOException {
        Set<String> added1 = newHashSet("a", "z", "e", "b");
        File f1 = assertWrite(added1.iterator(), false, added1.size());

        Set<String> added2 = newHashSet("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = newHashSet("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        append(newArrayList(f2, f3), f1, false);

        assertEquals(union(union(added1, added2), added3),
            readStringsAsSet(new FileInputStream(f1), false));
        assertTrue(f2.exists());
        assertTrue(f3.exists());
        assertTrue(f1.exists());
    }

    @Test
    public void appendTestFileDeleteOnError() throws IOException {
        Set<String> added2 = newHashSet("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = newHashSet("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        try {
            append(newArrayList(f2, f3), null, true);
        } catch (Exception e) {
        }
        assertTrue(!f2.exists());
        assertTrue(!f3.exists());
    }

    @Test
    public void appendRandomizedTest() throws Exception {
        Set<String> added1 = newHashSet();
        for (int i = 0; i < 100; i++) {
            added1.add(getRandomTestString());
        }
        File f1 = assertWrite(added1.iterator(), true, added1.size());

        Set<String> added2 = newHashSet("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), true, added2.size());

        append(newArrayList(f2), f1, true);

        assertEquals(union(added1, added2),
            readStringsAsSet(new FileInputStream(f1), true));
    }

    @Test
    public void appendWithLineBreaksTest() throws IOException {
        Set<String> added1 = newHashSet(getLineBreakStrings());
        File f1 = assertWrite(added1.iterator(), true, added1.size());

        Set<String> added2 = newHashSet("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), true, added2.size());

        append(newArrayList(f1), f2, true);

        assertEquals(union(added1, added2), readStringsAsSet(new FileInputStream(f2), true));
    }

    @Test
    public void mergeWithErrorsTest() throws IOException {
        Set<String> added2 = newHashSet("2", "3", "5", "6");
        File f2 = assertWrite(added2.iterator(), false, added2.size());

        Set<String> added3 = newHashSet("t", "y", "8", "9");
        File f3 = assertWrite(added3.iterator(), false, added3.size());

        try {
            merge(newArrayList(f2, f3), null);
        } catch(Exception e) {}

        assertTrue(!f2.exists());
        assertTrue(!f3.exists());
    }

    @Test
    public void fileIteratorTest() throws Exception {
        Set<String> added = newHashSet("a", "z", "e", "b");
        File f = assertWrite(added.iterator(), false, added.size());

        BurnOnCloseFileIterator iterator =
            BurnOnCloseFileIterator.wrap(FileUtils.lineIterator(f, UTF_8.toString()));

        assertEquals(added, Sets.newHashSet(iterator));
        assertTrue(f.exists());
    }

    @Test
    public void fileIteratorBurnTest() throws Exception {
        Set<String> added = newHashSet("a", "z", "e", "b");
        File f = assertWrite(added.iterator(), false, added.size());

        BurnOnCloseFileIterator iterator =
            BurnOnCloseFileIterator.wrap(FileUtils.lineIterator(f, UTF_8.toString()), f);

        assertEquals(added, Sets.newHashSet(iterator));
        assertTrue(!f.exists());
    }

    @Test
    public void fileIteratorLineBreakTest() throws IOException {
        Set<String> added = newHashSet(getLineBreakStrings());
        File f = assertWrite(added.iterator(), true, added.size());

        BurnOnCloseFileIterator iterator =
            new BurnOnCloseFileIterator<String>(FileUtils.lineIterator(f, UTF_8.toString()), f,
                new Function<String, String>() {
                    @Nullable @Override public String apply(@Nullable String input) {
                        return unescapeLineBreaks(input);
                    }
                });

        assertEquals(added, Sets.newHashSet(iterator));
        assertTrue(!f.exists());
    }

    @Test
    public void fileIteratorRandomizedTest() throws Exception {
        Set<String> added = newHashSet();
        for (int i = 0; i < 100; i++) {
            added.add(getRandomTestString());
        }
        File f = assertWrite(added.iterator(), true, added.size());

        BurnOnCloseFileIterator iterator =
            new BurnOnCloseFileIterator<String>(FileUtils.lineIterator(f, UTF_8.toString()),
                f,
                new Function<String, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable String input) {
                        return unescapeLineBreaks(input);
                    }
                });

        assertEquals(added, Sets.newHashSet(iterator));
        assertTrue(!f.exists());
    }

    private static List<String> getLineBreakStrings() {
        return newArrayList("ab\nc\r", "ab\\z", "a\\\\z\nc",
            "/a", "/a/b\nc", "/a/b\rd", "/a/b\r\ne", "/a/c");
    }

    private static List<String> escape(List<String> list) {
        List<String> escaped = newArrayList();
        for (String s : list) {
            escaped.add(escapeLineBreak(s));
        }
        return escaped;
    }

    private static List<String> unescape(List<String> list) {
        List<String> unescaped = newArrayList();
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
}
