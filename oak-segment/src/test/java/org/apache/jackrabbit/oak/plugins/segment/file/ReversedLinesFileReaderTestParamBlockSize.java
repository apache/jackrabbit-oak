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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static org.apache.jackrabbit.oak.plugins.segment.file.ReversedLinesReaderTestData.GBK_BIN;
import static org.apache.jackrabbit.oak.plugins.segment.file.ReversedLinesReaderTestData.WINDOWS_31J_BIN;
import static org.apache.jackrabbit.oak.plugins.segment.file.ReversedLinesReaderTestData.X_WINDOWS_949_BIN;
import static org.apache.jackrabbit.oak.plugins.segment.file.ReversedLinesReaderTestData.X_WINDOWS_950_BIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test checks symmetric behaviour with  BufferedReader
 * FIXME: this is mostly taken from a copy of org.apache.commons.io.input
 * with a fix for IO-471. Replace again once commons-io has released a fixed version.
 */
@RunWith(Parameterized.class)
public class ReversedLinesFileReaderTestParamBlockSize {

    private static final String UTF_8 = "UTF-8";
    private static final String ISO_8859_1 = "ISO-8859-1";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File createFile(byte[] data) throws IOException {
        return ReversedLinesReaderTestData.createFile(folder.newFile(), data);
    }

    @SuppressWarnings("boxing")
    @Parameters // small and uneven block sizes are not used in reality but are good to show that the algorithm is solid
    public static Collection<Integer[]> blockSizes() {
            return Arrays.asList(new Integer[][] { {1}, {3}, {8}, {256}, {4096} });
    }

    private ReversedLinesFileReader reversedLinesFileReader;
    private final int testParamBlockSize;

    public ReversedLinesFileReaderTestParamBlockSize(Integer testWithBlockSize) {
        testParamBlockSize = testWithBlockSize;
    }

    // Strings are escaped in constants to avoid java source encoding issues (source file enc is UTF-8):

    // windows-31j characters
    private static final String TEST_LINE_WINDOWS_31J_1 = "\u3041\u3042\u3043\u3044\u3045";
    private static final String TEST_LINE_WINDOWS_31J_2 = "\u660E\u8F38\u5B50\u4EAC";
    // gbk characters (Simplified Chinese)
    private static final String TEST_LINE_GBK_1 = "\u660E\u8F38\u5B50\u4EAC";
    private static final String TEST_LINE_GBK_2 = "\u7B80\u4F53\u4E2D\u6587";
    // x-windows-949 characters (Korean)
    private static final String TEST_LINE_X_WINDOWS_949_1 = "\uD55C\uAD6D\uC5B4";
    private static final String TEST_LINE_X_WINDOWS_949_2 = "\uB300\uD55C\uBBFC\uAD6D";
    // x-windows-950 characters (Traditional Chinese)
    private static final String TEST_LINE_X_WINDOWS_950_1 = "\u660E\u8F38\u5B50\u4EAC";
    private static final String TEST_LINE_X_WINDOWS_950_2 = "\u7E41\u9AD4\u4E2D\u6587";

    @After
    public void closeReader() {
        try {
            reversedLinesFileReader.close();
        } catch(Exception e) {
            // ignore
        }
    }

    @Test
    public void testWindows31jFile() throws URISyntaxException, IOException {
        File testFileWindows31J = createFile(WINDOWS_31J_BIN);
        reversedLinesFileReader = new ReversedLinesFileReader(testFileWindows31J, testParamBlockSize, "windows-31j");
        assertEqualsAndNoLineBreaks(TEST_LINE_WINDOWS_31J_2, reversedLinesFileReader.readLine());
        assertEqualsAndNoLineBreaks(TEST_LINE_WINDOWS_31J_1, reversedLinesFileReader.readLine());
    }

    @Test
    public void testGBK() throws URISyntaxException, IOException {
        File testFileGBK = createFile(GBK_BIN);
        reversedLinesFileReader = new ReversedLinesFileReader(testFileGBK, testParamBlockSize, "GBK");
        assertEqualsAndNoLineBreaks(TEST_LINE_GBK_2, reversedLinesFileReader.readLine());
        assertEqualsAndNoLineBreaks(TEST_LINE_GBK_1, reversedLinesFileReader.readLine());
    }

    @Test
    public void testxWindows949File() throws URISyntaxException, IOException {
        File testFilexWindows949 = createFile(X_WINDOWS_949_BIN);
        reversedLinesFileReader = new ReversedLinesFileReader(testFilexWindows949, testParamBlockSize, "x-windows-949");
        assertEqualsAndNoLineBreaks(TEST_LINE_X_WINDOWS_949_2, reversedLinesFileReader.readLine());
        assertEqualsAndNoLineBreaks(TEST_LINE_X_WINDOWS_949_1, reversedLinesFileReader.readLine());
    }

    @Test
    public void testxWindows950File() throws URISyntaxException, IOException {
        File testFilexWindows950 = createFile(X_WINDOWS_950_BIN);
        reversedLinesFileReader = new ReversedLinesFileReader(testFilexWindows950, testParamBlockSize, "x-windows-950");
        assertEqualsAndNoLineBreaks(TEST_LINE_X_WINDOWS_950_2, reversedLinesFileReader.readLine());
        assertEqualsAndNoLineBreaks(TEST_LINE_X_WINDOWS_950_1, reversedLinesFileReader.readLine());
    }

    static void assertEqualsAndNoLineBreaks(String msg, String expected, String actual) {
        if(actual!=null) {
            assertFalse("Line contains \\n: line="+actual, actual.contains("\n"));
            assertFalse("Line contains \\r: line="+actual, actual.contains("\r"));
        }
        assertEquals(msg, expected, actual);
    }
    static void assertEqualsAndNoLineBreaks(String expected, String actual) {
        assertEqualsAndNoLineBreaks(null, expected, actual);
    }
}
