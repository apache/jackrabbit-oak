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

package org.apache.jackrabbit.oak.plugins.blob;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector.FileLineDifferenceIterator;
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

}
