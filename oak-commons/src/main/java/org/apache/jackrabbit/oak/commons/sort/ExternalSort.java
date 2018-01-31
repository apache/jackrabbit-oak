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

// filename: ExternalSort.java
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.util.function.Function.identity;

/**
 * Source copied from a publicly available library.
 * @see <a href="https://code.google.com/p/externalsortinginjava/">https://code.google.com/p/externalsortinginjava</a>
 *
 * <pre>
 * Goal: offer a generic external-memory sorting program in Java.
 * 
 * It must be : - hackable (easy to adapt) - scalable to large files - sensibly efficient.
 * 
 * This software is in the public domain.
 * 
 * Usage: java org/apache/oak/commons/sort//ExternalSort somefile.txt out.txt
 * 
 * You can change the default maximal number of temporary files with the -t flag: java
 * org/apache/oak/commons/sort/ExternalSort somefile.txt out.txt -t 3
 * 
 * You can change the default maximum memory available with the -m flag: java
 * org/apache/oak/commons/sort/ExternalSort somefile.txt out.txt -m 8192
 * 
 * For very large files, you might want to use an appropriate flag to allocate more memory to
 * the Java VM: java -Xms2G org/apache/oak/commons/sort/ExternalSort somefile.txt out.txt
 * 
 * By (in alphabetical order) Philippe Beaudoin, Eleftherios Chetzakis, Jon Elsas, Christan
 * Grant, Daniel Haran, Daniel Lemire, Sugumaran Harikrishnan, Jerry Yang, First published:
 * April 2010 originally posted at
 * http://lemire.me/blog/archives/2010/04/01/external-memory-sorting-in-java/
 * </pre>
 */
public class ExternalSort {

    /*
     * This sorts a file (input) to an output file (output) using default parameters
     * 
     * @param file source file
     * 
     * @param file output file
     */
    public static void sort(File input, File output) throws IOException {
        ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(input), output);
    }
    
    static final int DEFAULTMAXTEMPFILES = 1024;
    
    /**
    * Defines the default maximum memory to be used while sorting (8 MB)
    */
    static final long DEFAULT_MAX_MEM_BYTES = 8388608L;
    
    // we divide the file into small blocks. If the blocks
    // are too small, we shall create too many temporary files.
    // If they are too big, we shall be using too much memory.
    public static long estimateBestSizeOfBlocks(File filetobesorted,
            int maxtmpfiles, long maxMemory) {
        long sizeoffile = filetobesorted.length() * 2;
        return estimateBestSizeOfBlocks(sizeoffile, maxtmpfiles, maxMemory);

    }

    private static long estimateBestSizeOfBlocks(long sizeoffile, int maxtmpfiles, long maxMemory) {
        /**
         * We multiply by two because later on someone insisted on counting the memory usage as 2
         * bytes per character. By this model, loading a file with 1 character will use 2 bytes.
         */
        // we don't want to open up much more than maxtmpfiles temporary
        // files, better run
        // out of memory first.
        long blocksize = sizeoffile / maxtmpfiles
                + (sizeoffile % maxtmpfiles == 0 ? 0 : 1);

        // on the other hand, we don't want to create many temporary
        // files
        // for naught. If blocksize is less than maximum allowed memory,
        // scale the blocksize to be equal to the maxMemory parameter

        if (blocksize < maxMemory) {
            blocksize = maxMemory;
        }
        return blocksize;
    }

    /**
     * This will simply load the file by blocks of lines, then sort them in-memory, and write the
     * result to temporary files that have to be merged later.
     * 
     * @param file
     *            some flat file
     * @return a list of temporary flat files
     */
    public static List<File> sortInBatch(File file)
            throws IOException {
        return sortInBatch(file, defaultcomparator, DEFAULTMAXTEMPFILES, DEFAULT_MAX_MEM_BYTES,
                Charset.defaultCharset(), null, false);
    }

    /**
     * This will simply load the file by blocks of lines, then sort them in-memory, and write the
     * result to temporary files that have to be merged later.
     * 
     * @param file
     *            some flat file
     * @param cmp
     *            string comparator
     * @return a list of temporary flat files
     */
    public static List<File> sortInBatch(File file, Comparator<String> cmp)
            throws IOException {
        return sortInBatch(file, cmp, DEFAULTMAXTEMPFILES, DEFAULT_MAX_MEM_BYTES,
                Charset.defaultCharset(), null, false);
    }

    /**
     * This will simply load the file by blocks of lines, then sort them in-memory, and write the
     * result to temporary files that have to be merged later.
     * 
     * @param file
     *            some flat file
     * @param cmp
     *            string comparator
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded.
     * @return a list of temporary flat files
     */
    public static List<File> sortInBatch(File file, Comparator<String> cmp,
            boolean distinct) throws IOException {
        return sortInBatch(file, cmp, DEFAULTMAXTEMPFILES, DEFAULT_MAX_MEM_BYTES,
                Charset.defaultCharset(), null, distinct);
    }

    /**
     * This will simply load the file by blocks of lines, then sort them in-memory, and write the
     * result to temporary files that have to be merged later. You can specify a bound on the number
     * of temporary files that will be created.
     *
     * @param file
     *            some flat file
     * @param cmp
     *            string comparator
     * @param maxtmpfiles
     *            maximal number of temporary files
     * @param cs
     *            character set to use (can use Charset.defaultCharset())
     * @param tmpdirectory
     *            location of the temporary files (set to null for default location)
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded.
     * @param numHeader
     *            number of lines to preclude before sorting starts
     * @param usegzip use gzip compression for the temporary files
     * @return a list of temporary flat files
     */
    public static List<File> sortInBatch(File file, Comparator<String> cmp,
                                         int maxtmpfiles, long maxMemory, Charset cs, File tmpdirectory,
                                         boolean distinct, int numHeader, boolean usegzip)
            throws IOException {
        return sortInBatch(file, cmp, maxtmpfiles, maxMemory, cs, tmpdirectory, distinct,
                numHeader, usegzip, identity(), identity());
    }

    /**
     * This will simply load the file by blocks of lines, then sort them in-memory, and write the
     * result to temporary files that have to be merged later. You can specify a bound on the number
     * of temporary files that will be created.
     *
     * @param file
     *            some flat file
     * @param cmp
     *            string comparator
     * @param maxtmpfiles
     *            maximal number of temporary files
     * @param cs
     *            character set to use (can use Charset.defaultCharset())
     * @param tmpdirectory
     *            location of the temporary files (set to null for default location)
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded.
     * @param numHeader
     *            number of lines to preclude before sorting starts
     * @param usegzip use gzip compression for the temporary files
     * @param typeToString
     *            function to map string to custom type. User for coverting line to custom type for the
     *            purpose of sorting
     * @param stringToType
     *          function to map custom type to string. Used for storing sorted content back to file
     * @return a list of temporary flat files
     */
    public static <T> List<File> sortInBatch(File file, Comparator<T> cmp,
                                             int maxtmpfiles, long maxMemory, Charset cs, File tmpdirectory,
                                             boolean distinct, int numHeader, boolean usegzip,
                                             Function<T, String> typeToString, Function<String, T> stringToType)
            throws IOException {
        // in bytes
        long blocksize = estimateBestSizeOfBlocks(file, maxtmpfiles, maxMemory);
        try (BufferedReader fbr = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), cs))) {
            return sortInBatch(fbr, blocksize, cmp, cs, tmpdirectory, distinct, numHeader, usegzip, typeToString, stringToType);
        }
    }

    public static <T> List<File> sortInBatch(BufferedReader fbr, long actualFileSize, Comparator<T> cmp,
                                             int maxtmpfiles, long maxMemory, Charset cs, File tmpdirectory,
                                             boolean distinct, int numHeader, boolean usegzip,
                                             Function<T, String> typeToString, Function<String, T> stringToType)
            throws IOException {
        // in bytes
        long blocksize = estimateBestSizeOfBlocks(actualFileSize, maxtmpfiles, maxMemory);
        try {
            return sortInBatch(fbr, blocksize, cmp, cs, tmpdirectory, distinct, numHeader, usegzip, typeToString, stringToType);
        } finally {
            fbr.close();
        }
    }

    /**
     * This will simply load the file by blocks of lines, then sort them in-memory, and write the
     * result to temporary files that have to be merged later. You can specify a bound on the number
     * of temporary files that will be created.
     * 
     *
     * @param fbr
     *            buffered read for file to be sorted
     * @param cmp
     *            string comparator
     * @param maxtmpfiles
     *            maximal number of temporary files
     * @param cs
     *            character set to use (can use Charset.defaultCharset())
     * @param tmpdirectory
     *            location of the temporary files (set to null for default location)
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded.
     * @param numHeader
     *            number of lines to preclude before sorting starts
     * @param usegzip use gzip compression for the temporary files
     * @param typeToString
     *            function to map string to custom type. User for coverting line to custom type for the
     *            purpose of sorting
     * @param stringToType
     *          function to map custom type to string. Used for storing sorted content back to file
     * @return a list of temporary flat files
     */
    private static <T> List<File> sortInBatch(BufferedReader fbr, long blocksize, Comparator<T> cmp,
                                             Charset cs, File tmpdirectory,
                                             boolean distinct, int numHeader, boolean usegzip,
                                             Function<T, String> typeToString, Function<String, T> stringToType)
            throws IOException {
        List<File> files = new ArrayList<File>();
        try {
            List<T> tmplist = new ArrayList<>();
            String line = "";
            try {
                int counter = 0;
                while (line != null) {
                    // in bytes
                    long currentblocksize = 0;
                    while ((currentblocksize < blocksize)
                            && ((line = fbr.readLine()) != null)) {
                        // as long as you have enough memory
                        if (counter < numHeader) {
                            counter++;
                            continue;
                        }
                        tmplist.add(stringToType.apply(line));
                        // ram usage estimation, not
                        // very accurate, still more
                        // realistic that the simple 2 *
                        // String.length
                        currentblocksize += StringSizeEstimator
                                .estimatedSizeOf(line);
                    }
                    files.add(sortAndSave(tmplist, cmp, cs,
                            tmpdirectory, distinct, usegzip, typeToString));
                    tmplist.clear();
                }
            } catch (EOFException oef) {
                if (tmplist.size() > 0) {
                    files.add(sortAndSave(tmplist, cmp, cs,
                            tmpdirectory, distinct, usegzip, typeToString));
                    tmplist.clear();
                }
            }
        } finally {
            fbr.close();
        }
        return files;
    }

    /**
     * This will simply load the file by blocks of lines, then sort them in-memory, and write the
     * result to temporary files that have to be merged later. You can specify a bound on the number
     * of temporary files that will be created.
     * 
     * @param file
     *            some flat file
     * @param cmp
     *            string comparator
     * @param maxtmpfiles
     *            maximal number of temporary files
     * @param cs
     *            character set to use (can use Charset.defaultCharset())
     * @param tmpdirectory
     *            location of the temporary files (set to null for default location)
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded.
     * @return a list of temporary flat files
     */
    public static List<File> sortInBatch(File file, Comparator<String> cmp,
            int maxtmpfiles, long maxMemory, Charset cs, File tmpdirectory, boolean distinct)
            throws IOException {
        return sortInBatch(file, cmp, maxtmpfiles, maxMemory, cs, tmpdirectory,
                distinct, 0, false);
    }

    /**
     * Sort a list and save it to a temporary file
     *
     * @return the file containing the sorted data
     * @param tmplist
     *            data to be sorted
     * @param cmp
     *            string comparator
     * @param cs
     *            charset to use for output (can use Charset.defaultCharset())
     * @param tmpdirectory
     *            location of the temporary files (set to null for default location)
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded.
     */
    public static File sortAndSave(List<String> tmplist,
                                   Comparator<String> cmp, Charset cs, File tmpdirectory,
                                   boolean distinct, boolean usegzip) throws IOException {
        return sortAndSave(tmplist, cmp, cs, tmpdirectory, distinct, usegzip, identity());
    }

    /**
     * Sort a list and save it to a temporary file
     * 
     * @return the file containing the sorted data
     * @param tmplist
     *            data to be sorted
     * @param cmp
 *            string comparator
     * @param cs
*            charset to use for output (can use Charset.defaultCharset())
     * @param tmpdirectory
*            location of the temporary files (set to null for default location)
     * @param distinct
     * @param typeToString
     *        function to map string to custom type. User for coverting line to custom type for the
     *        purpose of sorting
     */
    public static <T> File sortAndSave(List<T> tmplist,
                                   Comparator<T> cmp, Charset cs, File tmpdirectory,
                                   boolean distinct, boolean usegzip, Function<T, String> typeToString) throws IOException {
        Collections.sort(tmplist, cmp);
        File newtmpfile = File.createTempFile("sortInBatch",
                "flatfile", tmpdirectory);
        newtmpfile.deleteOnExit();
        OutputStream out = new FileOutputStream(newtmpfile);
        int zipBufferSize = 2048;
        if (usegzip) {
            out = new GZIPOutputStream(out, zipBufferSize) {
                {
                    def.setLevel(Deflater.BEST_SPEED);
                }
            };
        }
        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                out, cs));
        T lastLine = null;
        try {
            for (T r : tmplist) {
                // Skip duplicate lines
                if (!distinct || (lastLine == null || (lastLine != null && cmp.compare(r, lastLine) != 0))) {
                    fbw.write(typeToString.apply(r));
                    fbw.newLine();
                    lastLine = r;
                }
            }
        } finally {
            fbw.close();
        }
        return newtmpfile;
    }

    /**
     * Sort a list and save it to a temporary file
     * 
     * @return the file containing the sorted data
     * @param tmplist
     *            data to be sorted
     * @param cmp
     *            string comparator
     * @param cs
     *            charset to use for output (can use Charset.defaultCharset())
     * @param tmpdirectory
     *            location of the temporary files (set to null for default location)
     */
    public static File sortAndSave(List<String> tmplist,
            Comparator<String> cmp, Charset cs, File tmpdirectory)
            throws IOException {
        return sortAndSave(tmplist, cmp, cs, tmpdirectory, false, false);
    }

    /**
     * This merges a bunch of temporary flat files
     * 
     * @param files
     * @param outputfile
     *            file
     * @return The number of lines sorted. (P. Beaudoin)
     */
    public static int mergeSortedFiles(List<File> files, File outputfile) throws IOException {
        return mergeSortedFiles(files, outputfile, defaultcomparator,
                Charset.defaultCharset());
    }

    /**
     * This merges a bunch of temporary flat files
     * 
     * @param files
     * @param outputfile
     *            file
     * @return The number of lines sorted. (P. Beaudoin)
     */
    public static int mergeSortedFiles(List<File> files, File outputfile,
            final Comparator<String> cmp) throws IOException {
        return mergeSortedFiles(files, outputfile, cmp,
                Charset.defaultCharset());
    }

    /**
     * This merges a bunch of temporary flat files
     * 
     * @param files
     * @param outputfile
     *            file
     * @return The number of lines sorted. (P. Beaudoin)
     */
    public static int mergeSortedFiles(List<File> files, File outputfile,
            final Comparator<String> cmp, boolean distinct)
            throws IOException {
        return mergeSortedFiles(files, outputfile, cmp,
                Charset.defaultCharset(), distinct);
    }

    /**
     * This merges a bunch of temporary flat files
     *
     * @param files
     *            The {@link List} of sorted {@link File}s to be merged.
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded. (elchetz@gmail.com)
     * @param outputfile
     *            The output {@link File} to merge the results to.
     * @param cmp
     *            The {@link Comparator} to use to compare {@link String}s.
     * @param cs
     *            The {@link Charset} to be used for the byte to character conversion.
     * @param append
     *            Pass <code>true</code> if result should append to {@link File} instead of
     *            overwrite. Default to be false for overloading methods.
     * @param usegzip
     *            assumes we used gzip compression for temporary files
     * @return The number of lines sorted. (P. Beaudoin)
     * @since v0.1.4
     */
    public static <T> int mergeSortedFiles(List<File> files, File outputfile,
                                           final Comparator<String> cmp, Charset cs, boolean distinct,
                                           boolean append, boolean usegzip) throws IOException {
        return mergeSortedFiles(files, outputfile, cmp, cs, distinct, append, usegzip, Function.identity(), Function.identity());
    }

    /**
     * This merges a bunch of temporary flat files and deletes them on success or error.
     *
     * @param files
     *            The {@link List} of sorted {@link File}s to be merged.
     * @param outputfile
     *            The output {@link File} to merge the results to.
     * @param cmp
     *            The {@link Comparator} to use to compare {@link String}s.
     * @param cs
     *            The {@link Charset} to be used for the byte to character conversion.
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded. (elchetz@gmail.com)
     * @param append
     *            Pass <code>true</code> if result should append to {@link File} instead of
     *            overwrite. Default to be false for overloading methods.
     * @param usegzip
     *            assumes we used gzip compression for temporary files
     * @param typeToString
     *            function to map string to custom type. User for coverting line to custom type for the
     *            purpose of sorting
     * @param stringToType
     *          function to map custom type to string. Used for storing sorted content back to file
     * @since v0.1.4
     */
    public static <T> int mergeSortedFiles(List<File> files, File outputfile,
                                           final Comparator<T> cmp, Charset cs, boolean distinct,
                                           boolean append, boolean usegzip, Function<T, String> typeToString,
                                           Function<String, T> stringToType) throws IOException {
        boolean success = false;
        try (BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfile, append), cs))){
            int result = mergeSortedFiles(files, fbw, cmp, cs, distinct, usegzip, typeToString, stringToType);
            success = true;
            return result;
        } finally {
            if (!success) {
                for (File f : files) {
                    if (f.exists()) {
                        f.delete();
                    }
                }
            }
        }
    }

    /**
     * This merges a bunch of temporary flat files and deletes them on success or error.
     * 
     * @param files
     *            The {@link List} of sorted {@link File}s to be merged.
     * @param fbw
     *            Buffered writer used to store the sorted content
     * @param cmp
     *            The {@link Comparator} to use to compare {@link String}s.
     * @param cs
     *            The {@link Charset} to be used for the byte to character conversion.
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded. (elchetz@gmail.com)
     * @param usegzip
     *            assumes we used gzip compression for temporary files
     * @param typeToString
     *            function to map string to custom type. User for coverting line to custom type for the
     *            purpose of sorting
     * @param stringToType
     *          function to map custom type to string. Used for storing sorted content back to file
     * @since v0.1.4
     */
    public static <T> int mergeSortedFiles(List<File> files,
                                           BufferedWriter fbw, final Comparator<T> cmp, Charset cs, boolean distinct,
                                           boolean usegzip, Function<T, String> typeToString,
                                           Function<String, T> stringToType) throws IOException {
        ArrayList<BinaryFileBuffer<T>> bfbs = new ArrayList<>();
        try {
            for (File f : files) {
                final int bufferSize = 2048;
                InputStream in = new FileInputStream(f);
                BufferedReader br;
                if (usegzip) {
                    br = new BufferedReader(new InputStreamReader(new GZIPInputStream(in, bufferSize), cs));
                } else {
                    br = new BufferedReader(new InputStreamReader(in, cs));
                }

                BinaryFileBuffer<T> bfb = new BinaryFileBuffer<>(br, stringToType);
                bfbs.add(bfb);
            }
            int rowcounter = merge(fbw, cmp, distinct, bfbs, typeToString);
            return rowcounter;
        } finally {
            for (BinaryFileBuffer buffer : bfbs) {
                try {
                    buffer.close();
                } catch (Exception e) {}
            }
            for (File f : files) {
                f.delete();
            }
        }
    }

    /**
     * This merges several BinaryFileBuffer to an output writer.
     * 
     * @param fbw
     *            A buffer where we write the data.
     * @param cmp
     *            A comparator object that tells us how to sort the lines.
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded. (elchetz@gmail.com)
     * @param buffers
     *            Where the data should be read.
     * @param typeToString
     *            function to map string to custom type. User for coverting line to custom type for the
     *            purpose of sorting
     *
     */
    public static <T> int merge(BufferedWriter fbw, final Comparator<T> cmp, boolean distinct,
                            List<BinaryFileBuffer<T>> buffers, Function<T, String> typeToString)
            throws IOException {
        PriorityQueue<BinaryFileBuffer<T>> pq = new PriorityQueue<>(
                11, new Comparator<BinaryFileBuffer<T>>() {
                    @Override
                    public int compare(BinaryFileBuffer<T> i,
                            BinaryFileBuffer<T> j) {
                        return cmp.compare(i.peek(), j.peek());
                    }
                });
        for (BinaryFileBuffer bfb : buffers) {
            if (!bfb.empty()) {
                pq.add(bfb);
            }
        }
        int rowcounter = 0;
        T lastLine = null;
        try {
            while (pq.size() > 0) {
                BinaryFileBuffer<T> bfb = pq.poll();
                T r = bfb.pop();
                // Skip duplicate lines
                if (!distinct || (lastLine == null || (lastLine != null && cmp.compare(r, lastLine) != 0))) {
                    fbw.write(typeToString.apply(r));
                    fbw.newLine();
                    lastLine = r;
                }
                ++rowcounter;
                if (bfb.empty()) {
                    bfb.fbr.close();
                } else {
                    pq.add(bfb); // add it back
                }
            }
        } finally {
            fbw.close();
            for (BinaryFileBuffer bfb : buffers) {
                bfb.close();
            }
        }
        return rowcounter;

    }

    /**
     * This merges a bunch of temporary flat files
     * 
     * @param files
     *            The {@link List} of sorted {@link File}s to be merged.
     * @param distinct
     *            Pass <code>true</code> if duplicate lines should be discarded. (elchetz@gmail.com)
     * @param outputfile
     *            The output {@link File} to merge the results to.
     * @param cmp
     *            The {@link Comparator} to use to compare {@link String}s.
     * @param cs
     *            The {@link Charset} to be used for the byte to character conversion.
     * @return The number of lines sorted. (P. Beaudoin)
     * @since v0.1.2
     */
    public static int mergeSortedFiles(List<File> files, File outputfile,
            final Comparator<String> cmp, Charset cs, boolean distinct)
            throws IOException {
        return mergeSortedFiles(files, outputfile, cmp, cs, distinct,
                false, false);
    }

    /**
     * This merges a bunch of temporary flat files
     * 
     * @param files
     * @param outputfile
     *            file
     * @param cs
     *            character set to use to load the strings
     * @return The number of lines sorted. (P. Beaudoin)
     */
    public static int mergeSortedFiles(List<File> files, File outputfile,
            final Comparator<String> cmp, Charset cs) throws IOException {
        return mergeSortedFiles(files, outputfile, cmp, cs, false);
    }

    public static void displayUsage() {
        System.out
                .println("java com.google.externalsorting.ExternalSort inputfile outputfile");
        System.out.println("Flags are:");
        System.out.println("-v or --verbose: verbose output");
        System.out.println("-d or --distinct: prune duplicate lines");
        System.out
                .println("-t or --maxtmpfiles (followed by an integer): specify an upper bound on the number of temporary files");
        System.out
                .println("-m or --maxmembytes (followed by a long): specify an upper bound on the memory");
        System.out
                .println("-c or --charset (followed by a charset code): specify the character set to use (for sorting)");
        System.out
                .println("-z or --gzip: use compression for the temporary files");
        System.out
                .println("-H or --header (followed by an integer): ignore the first few lines");
        System.out
                .println("-s or --store (following by a path): where to store the temporary files");
        System.out.println("-h or --help: display this message");
    }

    public static void main(String[] args) throws IOException {
        boolean verbose = false;
        boolean distinct = false;
        int maxtmpfiles = DEFAULTMAXTEMPFILES;
        long maxMemory = DEFAULT_MAX_MEM_BYTES;        
        Charset cs = Charset.defaultCharset();
        String inputfile = null, outputfile = null;
        File tempFileStore = null;
        boolean usegzip = false;
        int headersize = 0;
        for (int param = 0; param < args.length; ++param) {
            if (args[param].equals("-v")
                    || args[param].equals("--verbose")) {
                verbose = true;
            } else if (args[param].equals("-h") || args[param]
                    .equals("--help")) {
                displayUsage();
                return;
            } else if (args[param].equals("-d") || args[param]
                    .equals("--distinct")) {
                distinct = true;
            } else if ((args[param].equals("-t") || args[param]
                    .equals("--maxtmpfiles"))
                    && args.length > param + 1) {
                param++;
                maxtmpfiles = Integer.parseInt(args[param]);
                if (headersize < 0) {
                    System.err
                            .println("maxtmpfiles should be positive");
                }
            } else if ((args[param].equals("-m") || args[param]
                        .equals("--maxmembytes"))
                        && args.length > param + 1) {
                    param++;
                    maxMemory = Long.parseLong(args[param]);
                    if (headersize < 0) {
                        System.err
                            .println("maxmembytes should be positive");
                    }
            } else if ((args[param].equals("-c") || args[param]
                    .equals("--charset"))
                    && args.length > param + 1) {
                param++;
                cs = Charset.forName(args[param]);
            } else if (args[param].equals("-z") || args[param]
                    .equals("--gzip")) {
                usegzip = true;
            } else if ((args[param].equals("-H") || args[param]
                    .equals("--header")) && args.length > param + 1) {
                param++;
                headersize = Integer.parseInt(args[param]);
                if (headersize < 0) {
                    System.err
                            .println("headersize should be positive");
                }
            } else if ((args[param].equals("-s") || args[param]
                    .equals("--store")) && args.length > param + 1) {
                param++;
                tempFileStore = new File(args[param]);
            } else {
                if (inputfile == null) {
                    inputfile = args[param];
                } else if (outputfile == null) {
                    outputfile = args[param];
                } else {
                    System.out.println("Unparsed: "
                            + args[param]);
                }
            }
        }
        if (outputfile == null) {
            System.out
                    .println("please provide input and output file names");
            displayUsage();
            return;
        }
        Comparator<String> comparator = defaultcomparator;
        List<File> l = sortInBatch(new File(inputfile), comparator,
                maxtmpfiles, maxMemory, cs, tempFileStore, distinct, headersize,
                usegzip);
        if (verbose) {
            System.out
                    .println("created " + l.size() + " tmp files");
        }
        mergeSortedFiles(l, new File(outputfile), comparator, cs,
                distinct, false, usegzip);
    }

    public static Comparator<String> defaultcomparator = new Comparator<String>() {
        @Override
        public int compare(String r1, String r2) {
            return r1.compareTo(r2);
        }
    };
}

class BinaryFileBuffer<T> {
    public final BufferedReader fbr;
    private final Function<String, T> stringToType;
    private T cache;
    private boolean empty;

    public BinaryFileBuffer(BufferedReader r, Function<String, T> stringToType)
            throws IOException {
        this.fbr = r;
        this.stringToType = stringToType;
        reload();
    }

    public boolean empty() {
        return this.empty;
    }

    private void reload() throws IOException {
        try {
            if ((this.cache = stringToType.apply(fbr.readLine())) == null) {
                this.empty = true;
                this.cache = null;
            } else {
                this.empty = false;
            }
        } catch (EOFException oef) {
            this.empty = true;
            this.cache = null;
        }
    }

    public void close() throws IOException {
        this.fbr.close();
    }

    public T peek() {
        if (empty()) {
            return null;
        }
        return this.cache;
    }

    public T pop() throws IOException {
        T answer = peek();
        reload();
        return answer;
    }

}
