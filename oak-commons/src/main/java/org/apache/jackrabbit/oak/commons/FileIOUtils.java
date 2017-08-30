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
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.io.Closeables.close;
import static com.google.common.io.FileWriteMode.APPEND;
import static com.google.common.io.Files.asByteSink;
import static com.google.common.io.Files.move;
import static com.google.common.io.Files.newWriter;
import static java.io.File.createTempFile;
import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.apache.commons.io.FileUtils.forceDelete;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.io.IOUtils.copyLarge;
import static org.apache.commons.io.LineIterator.closeQuietly;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.unescapeLineBreaks;
import static org.apache.jackrabbit.oak.commons.sort.ExternalSort.mergeSortedFiles;
import static org.apache.jackrabbit.oak.commons.sort.ExternalSort.sortInBatch;

/**
 * Simple File utils
 */
public final class FileIOUtils {

    private FileIOUtils() {
    }

    public final static Comparator<String> lexComparator = new Comparator<String>() {
        @Override public int compare(String s1, String s2) {
            return s1.compareTo(s2);
        }
    };

    /**
     * Sorts the given file externally using the {@link #lexComparator} and removes duplicates.
     *
     * @param file file whose contents needs to be sorted
     */
    public static void sort(File file) throws IOException {
        File sorted = createTempFile("fleioutilssort", null);
        merge(sortInBatch(file, lexComparator, true), sorted);
        move(sorted, file);
    }

    /**
     * Sorts the given file externally with the given comparator and removes duplicates.
     *
     * @param file file whose contents needs to be sorted
     * @param comparator to compare
     * @throws IOException
     */
    public static void sort(File file, Comparator<String> comparator) throws IOException {
        File sorted = createTempFile("fleioutilssort", null);
        merge(sortInBatch(file, comparator, true), sorted, comparator);
        move(sorted, file);
    }

    /**
     * Merges a list of files after sorting with the {@link #lexComparator}.
     *
     * @param files files to merge
     * @param output merge output file
     * @throws IOException
     */
    public static void merge(List<File> files, File output) throws IOException {
        mergeSortedFiles(
            files,
            output, lexComparator, true);
    }

    /**
     * Merges a list of files after sorting with the given comparator.
     *
     * @param files files to merge
     * @param output merge output file
     * @throws IOException
     */
    public static void merge(List<File> files, File output, Comparator<String> comparator) throws IOException {
        mergeSortedFiles(
            files,
            output, comparator, true);
    }

    /**

     * Copies an input stream to a file.
     *
     * @param stream steam to copy
     * @return
     * @throws IOException
     */
    public static File copy(InputStream stream) throws IOException {
        File file = createTempFile("fleioutilscopy", null);
        copyInputStreamToFile(stream, file);
        return file;
    }

    /**
     * Appends the contents of the list of files to the given file and deletes the files
     * if the delete flag is enabled.
     *
     * If there is a scope for lines in the files containing line break characters it should be
     * ensured that the files are written with {@link #writeAsLine(BufferedWriter, String, boolean)}
     * with true to escape line break characters.
     * @param files
     * @param appendTo
     * @throws IOException
     */
    public static void append(List<File> files, File appendTo, boolean delete) throws IOException {
        OutputStream appendStream = null;
        boolean threw = true;

        try {
            appendStream = asByteSink(appendTo, APPEND).openBufferedStream();

            for (File f : files) {
                InputStream iStream = new FileInputStream(f);
                try {
                    copyLarge(iStream, appendStream);
                } finally {
                    closeQuietly(iStream);
                }
            }
            if (delete) {
                for (File f : files) {
                    f.delete();
                }
            }
            threw = false;
        } finally {
            close(appendStream, threw);
        }
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
        return writeStrings(iterator, f, escape, null, "");
    }

    /**
     * Writes string from the given iterator to the given file and optionally
     * escape the written strings for line breaks.
     *
     * @param iterator the source of the strings
     * @param f file to write to
     * @param escape escape whether to escape for line breaks
     * @param logger logger to log progress
     * @param message message to log
     * @return
     * @throws IOException
     */
    public static int writeStrings(Iterator<String> iterator, File f, boolean escape,
        @Nullable Logger logger, @Nullable String message) throws IOException {
        BufferedWriter writer =  newWriter(f, UTF_8);
        boolean threw = true;

        int count = 0;
        try {
            while (iterator.hasNext()) {
                writeAsLine(writer, iterator.next(), escape);
                count++;
                if (logger != null) {
                    if (count % 1000 == 0) {
                        logger.info(Strings.nullToEmpty(message) + count);
                    }
                }
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

    /**
     * FileLineDifferenceIterator class which iterates over the difference of 2 files line by line.
     *
     * If there is a scope for lines in the files containing line break characters it should be
     * ensured that both the files are written with
     * {@link #writeAsLine(BufferedWriter, String, boolean)} with true to escape line break
     * characters.
     */
    public static class FileLineDifferenceIterator extends AbstractIterator<String> implements Closeable {
        private final PeekingIterator<String> peekMarked;
        private final LineIterator marked;
        private final LineIterator all;
        private Function<String, String> transformer = new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input;
            }
        };

        public FileLineDifferenceIterator(LineIterator marked, LineIterator available) throws IOException {
            this(marked, available, null);
        }

        public FileLineDifferenceIterator(File marked, File available,
            @Nullable Function<String, String> transformer) throws IOException {
            this(FileUtils.lineIterator(marked, UTF_8.toString()),
                FileUtils.lineIterator(available, UTF_8.toString()), transformer);
        }

        public FileLineDifferenceIterator(LineIterator marked, LineIterator available,
            @Nullable Function<String, String> transformer) throws IOException {
            this.marked = marked;
            this.peekMarked = Iterators.peekingIterator(marked);
            this.all = available;
            if (transformer != null) {
                this.transformer = transformer;
            }
        }

        @Override
        protected String computeNext() {
            String diff = computeNextDiff();
            if (diff == null) {
                close();
                return endOfData();
            }
            return diff;
        }

        @Override
        public void close() {
            LineIterator.closeQuietly(marked);
            LineIterator.closeQuietly(all);
        }

        private String computeNextDiff() {
            if (!all.hasNext()) {
                return null;
            }

            //Marked finish the rest of all are part of diff
            if (!peekMarked.hasNext()) {
                return all.next();
            }

            String diff = null;
            while (all.hasNext() && diff == null) {
                diff = all.next();
                while (peekMarked.hasNext()) {
                    String marked = peekMarked.peek();
                    int comparisonResult = transformer.apply(diff).compareTo(transformer.apply((marked)));
                    if (comparisonResult > 0) {
                        //Extra entries in marked. Ignore them and move on
                        peekMarked.next();
                    } else if (comparisonResult == 0) {
                        //Matching entry found in marked move past it. Not a
                        //dif candidate
                        peekMarked.next();
                        diff = null;
                        break;
                    } else {
                        //This entry is not found in marked entries
                        //hence part of diff
                        return diff;
                    }
                }
            }
            return diff;
        }
    }

    /**
     * Implements a {@link java.io.Closeable} wrapper over a {@link LineIterator}.
     * Also has a transformer to transform the output. If the underlying file is
     * provide then it deletes the file on {@link #close()}.
     *
     * If there is a scope for lines in the file containing line break characters it should be
     * ensured that the files is written with
     * {@link #writeAsLine(BufferedWriter, String, boolean)} with true to escape line break
     * characters and should be properly unescaped on read.
     * A custom transformer can also be provided to unescape.
     *
     * @param <T> the type of elements in the iterator
     */
    public static class BurnOnCloseFileIterator<T> extends AbstractIterator<T> implements Closeable {
        private final Logger log = LoggerFactory.getLogger(getClass());

        private final LineIterator iterator;
        private final Function<String, T> transformer;
        private File backingFile;

        public BurnOnCloseFileIterator(LineIterator iterator, Function<String, T> transformer) {
            this.iterator = iterator;
            this.transformer = transformer;
        }

        public BurnOnCloseFileIterator(LineIterator iterator, File backingFile,
            Function<String, T> transformer) {
            this.iterator = iterator;
            this.transformer = transformer;
            this.backingFile = backingFile;
        }

        @Override
        protected T computeNext() {
            if (iterator.hasNext()) {
                return transformer.apply(iterator.next());
            }

            try {
                close();
            } catch (IOException e) {
                log.warn("Error closing iterator", e);
            }
            return endOfData();
        }

        @Override
        public void close() throws IOException {
            closeQuietly(iterator);
            if (backingFile != null && backingFile.exists()) {
                forceDelete(backingFile);
            }
        }

        public static BurnOnCloseFileIterator<String> wrap(LineIterator iter) {
            return new BurnOnCloseFileIterator<String>(iter, new Function<String, String>() {
                public String apply(String s) {
                    return s;
                }
            });
        }

        public static BurnOnCloseFileIterator<String> wrap(LineIterator iter, File backingFile) {
            return new BurnOnCloseFileIterator<String>(iter, backingFile,
                new Function<String, String>() {
                    public String apply(String s) {
                        return s;
                    }
                });
        }
    }
}
