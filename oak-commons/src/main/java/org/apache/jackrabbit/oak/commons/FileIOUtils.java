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
package org.apache.jackrabbit.oak.commons;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.io.Closeables.close;
import static com.google.common.io.Files.newWriter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.unescapeLineBreaks;

/**
 * Simple File utils
 */
public final class FileIOUtils {

    private FileIOUtils() {
    }

    /**
     * Writes a string as a new line into the given buffered writer and optionally
     * escapes the line for line breaks.
     *
     * @param writer to write the string
     * @param str the string to write
     * @param escape whether to escape string for line breaks
     * @throws IOException
     */
    public static void writeAsLine(BufferedWriter writer, String str, boolean escape) throws IOException {
        if (escape) {
            writer.write(escapeLineBreak(str));
        } else {
            writer.write(str);
        }
        writer.newLine();
    }

    /**
     * Writes string from the given iterator to the given file and optionally
     * escape the written strings for line breaks.
     *
     * @param iterator the source of the strings
     * @param f file to write to
     * @param escape whether to escape for line breaks
     * @return count
     * @throws IOException
     */
    public static int writeStrings(Iterator<String> iterator, File f, boolean escape)
        throws IOException {
        BufferedWriter writer =  newWriter(f, UTF_8);
        boolean threw = true;

        int count = 0;
        try {
            while (iterator.hasNext()) {
                writeAsLine(writer, iterator.next(), escape);
                count++;
            }
            threw = false;
        } finally {
            close(writer, threw);
        }
        return count;
    }

    /**
     * Reads strings from the given stream into a set and optionally unescaping for line breaks.
     *
     * @param stream the source of the strings
     * @param unescape whether to unescape for line breaks
     * @return set
     * @throws IOException
     */
    public static Set<String> readStringsAsSet(InputStream stream, boolean unescape) throws IOException {
        BufferedReader reader = null;
        Set<String> set = newHashSet();
        boolean threw = true;

        try {
            reader = new BufferedReader(new InputStreamReader(stream, Charsets.UTF_8));
            String line  = null;
            while ((line = reader.readLine()) != null) {
                if (unescape) {
                    set.add(unescapeLineBreaks(line));
                } else {
                    set.add(line);
                }
            }
            threw = false;
        } finally {
            close(reader, threw);
        }
        return set;
    }

    /**
     * Composing iterator which unescapes for line breaks and delegates to the given comparator.
     * When using this it should be ensured that the data source has been correspondingly escaped.
     *
     * @param delegate the actual comparison iterator
     * @return comparator aware of line breaks
     */
    public static Comparator<String> lineBreakAwareComparator (Comparator<String> delegate) {
        return new FileIOUtils.TransformingComparator(delegate, new Function<String, String>() {
            @Nullable
            @Override
            public String apply(@Nullable String input) {
                return unescapeLineBreaks(input);
            }
        });
    }

    /**
     * Decorates the given comparator and applies the function before delegating to the decorated
     * comparator.
     */
    public static class TransformingComparator implements Comparator<String> {
        private Comparator delegate;
        private Function<String, String> func;

        public TransformingComparator(Comparator delegate, Function<String, String> func) {
            this.delegate = delegate;
            this.func = func;
        }

        @Override
        public int compare(String s1, String s2) {
            return delegate.compare(func.apply(s1), func.apply(s2));
        }
    }
}
