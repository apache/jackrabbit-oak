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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.commons.FileIOUtils.FileLineDifferenceIterator;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FileLineDifferenceIteratorTest {
    
    @Test
    public void testRandomized() throws Exception {
        Random r = new Random(0);
        for (int i = 0; i < 10000; i++) {
            TreeSet<String> marked = new TreeSet<String>();
            TreeSet<String> all = new TreeSet<String>();
            TreeSet<String> diff = new TreeSet<String>();
            int size = r.nextInt(5);
            for (int a = 0; a < size; a++) {
                marked.add("" + r.nextInt(10));
            }
            size = r.nextInt(5);
            for (int a = 0; a < size; a++) {
                all.add("" + r.nextInt(10));
            }
            diff.addAll(all);
            diff.removeAll(marked);
            String m = marked.toString().replaceAll("[ \\[\\]]", "");
            String a = all.toString().replaceAll("[ \\[\\]]", "");
            assertDiff(m, a,
                    new ArrayList<String>(diff));
        }
    }
    
    @Test
    public void testNoDiff() throws Exception {
        assertDiff("a,b,c", "a,b,c", Collections.<String> emptyList());
        assertDiff("a,b,c,d,f", "a,b,f", Collections.<String> emptyList());
    }

    @Test
    public void testSimpleDiff() throws Exception {
        assertDiff("a,b", "a,b,c", asList("c"));
        assertDiff("a,b", "", Collections.<String> emptyList());
        assertDiff("", "", Collections.<String> emptyList());
        assertDiff("", "a", asList("a"));
        assertDiff("", "a, b", asList("a", "b"));
    }

    @Test
    public void testDiffWithExtraEntriesInMarked() throws IOException {
        assertDiff("a,b", "a,b,c, e, h", asList("c", "e", "h"));
        assertDiff("a,b,d,e", "a,b,c", asList("c"));
        assertDiff("a,b,d,e,f", "a,b,c,f", asList("c"));
        assertDiff("a,b,d,e,f", "a,b,c,f, h", asList("c", "h"));
        assertDiff("3,7", "2,3,5,9", asList("2", "5", "9"));
    }
    
    @Test
    public void testMarkedDiffWithExtraEntriesInMarked() throws IOException {
        assertReverseDiff("a,b,c,e,h", "a,b,c", asList("e", "h"));
        assertReverseDiff("a,b,d,e", "a,b,c", asList("d", "e"));
        assertReverseDiff("a,b,d", "a,b,d", Collections.<String>emptyList());
        assertReverseDiff("a,0xb,d,e,f", "a,d", asList("0xb", "e", "f"));
        assertReverseDiff("a,0xb,d,e,f", "a,d,e,f,g", asList("0xb"));
    }

    @Test
    public void testDiffLineBreakChars() throws IOException {
        List<String> all = getLineBreakStrings();
        List<String> marked = getLineBreakStrings();
        remove(marked, 3, 2);

        // without escaping, the line breaks will be resolved
        assertDiff(Joiner.on(",").join(marked), Joiner.on(",").join(all),
            asList("/a", "c", "/a/b"));
    }

    @Test
    public void testDiffEscapedLineBreakChars() throws IOException {
        // Escaped characters
        List<String> all = escape(getLineBreakStrings());
        List<String> marked = escape(getLineBreakStrings());
        List<String> diff = remove(marked, 3, 2);

        assertDiff(Joiner.on(",").join(marked), Joiner.on(",").join(all), diff);
    }

    @Test
    public void testDiffTransform() throws IOException {
        assertTransformed("a:x,b:y", "a:1,b:2,c:3,e:4,h", asList("c:3", "e:4", "h"));
        assertTransformed("a,b,d,e", "a,b,c", asList("c"));
        assertTransformed("a:1,b:2,d:3,e:4,f:5", "a:z,b:y,c:x,f:w", asList("c:x"));
        assertTransformed("a,b,d,e,f", "a,b,c,f,h", asList("c", "h"));
        assertTransformed("3:1,7:6", "2:0,3:6,5:3,9:1", asList("2:0", "5:3", "9:1"));
        assertTransformed("", "", Collections.<String> emptyList());
        assertTransformed("", "a, b", asList("a", "b"));
        assertTransformed("", "a:4, b:1", asList("a:4", "b:1"));
    }

    private static List<String> getLineBreakStrings() {
        return Lists.newArrayList("ab\nc\r", "ab\\z", "a\\\\z\nc",
            "/a", "/a/b\nc", "/a/b\rd", "/a/b\r\ne", "/a/c");
    }

    private static List<String> remove(List<String> list, int idx, int count) {
        List<String> diff = Lists.newArrayList();
        int i = 0;
        while (i < count) {
            diff.add(list.remove(idx));
            i++;
        }
        return diff;
    }

    private static List<String> escape(List<String> list) {
        List<String> escaped = Lists.newArrayList();
        for (String s : list) {
            escaped.add(escapeLineBreak(s));
        }
        return escaped;
    }

    private static void assertReverseDiff(String marked, String all, List<String> diff) throws IOException {
        Iterator<String> itr = createItr(all, marked);
        assertThat("marked: " + marked + " all: " + all, ImmutableList.copyOf(itr), is(diff));
    }

    private static void assertDiff(String marked, String all, List<String> diff) throws IOException {
        Iterator<String> itr = createItr(marked, all);
        assertThat("marked: " + marked + " all: " + all, ImmutableList.copyOf(itr), is(diff));
    }

    private static Iterator<String> createItr(String marked, String all) throws IOException {
        return new FileLineDifferenceIterator(lineItr(marked), lineItr(all));
    }

    private static LineIterator lineItr(String seq) {
        Iterable<String> seqItr = Splitter.on(',').trimResults().split(seq);
        String lines = Joiner.on(StandardSystemProperty.LINE_SEPARATOR.value()).join(seqItr);
        return new LineIterator(new StringReader(lines));
    }

    private static void assertTransformed(String marked, String all, List<String> diff) throws IOException {
        Iterator<String> itr = new FileLineDifferenceIterator(lineItr(marked), lineItr(all),
            new Function<String, String>() {
                @Nullable @Override
                public String apply(@Nullable String input) {
                    if (input != null) {
                        return input.split(":")[0];
                    }
                    return null;
                }
            });

        assertThat("marked: " + marked + " all: " + all, ImmutableList.copyOf(itr), is(diff));
    }
}
