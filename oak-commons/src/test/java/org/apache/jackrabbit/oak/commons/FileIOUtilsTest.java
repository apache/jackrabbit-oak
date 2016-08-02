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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.readStringsAsSet;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.unescapeLineBreaks;


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
        File f = folder.newFile();

        int count = writeStrings(added.iterator(), f, false);
        Assert.assertEquals(added.size(), count);

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), false);

        Assert.assertEquals(added, retrieved);
    }

    @Test
    public void writeReadStringsWithLineBreaks() throws IOException {
        Set<String> added = newHashSet(getLineBreakStrings());
        File f = folder.newFile();
        int count = writeStrings(added.iterator(), f, true);
        Assert.assertEquals(added.size(), count);

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), true);
        Assert.assertEquals(added, retrieved);
    }

    @Test
    public void writeReadRandomStrings() throws Exception {
        Set<String> added = newHashSet();
        File f = folder.newFile();

        for (int i = 0; i < 100; i++) {
            added.add(getRandomTestString());
        }
        int count = writeStrings(added.iterator(), f, true);
        Assert.assertEquals(added.size(), count);

        Set<String> retrieved = readStringsAsSet(new FileInputStream(f), true);
        Assert.assertEquals(added, retrieved);
    }

    @Test
    public void compareWithLineBreaks() throws Exception {
        Comparator<String> lexCmp = new Comparator<String>() {
            @Override public int compare(String s1, String s2) {
                return s1.compareTo(s2);
            }
        };
        Comparator<String> cmp = FileIOUtils.lineBreakAwareComparator(lexCmp);

        List<String> strs = getLineBreakStrings();
        Collections.sort(strs, lexCmp);

        // Escape line breaks and then compare with string sorted
        List<String> escapedStrs = escape(getLineBreakStrings());
        Collections.sort(escapedStrs, cmp);

        Assert.assertEquals(strs, unescape(escapedStrs));
    }

    private static List<String> getLineBreakStrings() {
        return Lists.newArrayList("ab\nc\r", "ab\\z", "a\\\\z\nc",
            "/a", "/a/b\nc", "/a/b\rd", "/a/b\r\ne", "/a/c");
    }

    private static List<String> escape(List<String> list) {
        List<String> escaped = Lists.newArrayList();
        for (String s : list) {
            escaped.add(escapeLineBreak(s));
        }
        return escaped;
    }

    private static List<String> unescape(List<String> list) {
        List<String> unescaped = Lists.newArrayList();
        for (String s : list) {
            unescaped.add(unescapeLineBreaks(s));
        }
        return unescaped;
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
            CharsetEncoder encoder = Charset.forName(Charsets.UTF_8.toString()).newEncoder();
            try {
                encoder.encode(CharBuffer.wrap(s));
                valid = true;
            } catch (CharacterCodingException e) {
                buffer = new StringBuilder();
            }
        }
        return buffer.toString();
    }
}
