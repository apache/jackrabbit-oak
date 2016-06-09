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
package org.apache.jackrabbit.oak.segment.file;


import static org.apache.jackrabbit.oak.segment.file.ReversedLinesReaderTestData.GBK_BIN;
import static org.apache.jackrabbit.oak.segment.file.ReversedLinesReaderTestData.WINDOWS_31J_BIN;
import static org.apache.jackrabbit.oak.segment.file.ReversedLinesReaderTestData.X_WINDOWS_949_BIN;
import static org.apache.jackrabbit.oak.segment.file.ReversedLinesReaderTestData.X_WINDOWS_950_BIN;
import static org.apache.jackrabbit.oak.segment.file.ReversedLinesReaderTestData.createFile;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Stack;

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
public class ReversedLinesFileReaderTestParamFile {

    @Parameters
    public static Collection<Object[]> blockSizes() {
            return Arrays.asList(new Object[][] {
                    {WINDOWS_31J_BIN, "windows-31j", null},
                    {GBK_BIN, "gbk", null},
                    {X_WINDOWS_949_BIN, "x-windows-949", null},
                    {X_WINDOWS_950_BIN, "x-windows-950", null},
            });
    }

    private ReversedLinesFileReader reversedLinesFileReader;
    private BufferedReader bufferedReader;

    private final byte[] data;
    private final String encoding;
    private final int buffSize;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    public ReversedLinesFileReaderTestParamFile(byte[] data, String encoding, Integer buffSize) {
        this.data = data;
        this.encoding = encoding;
        this.buffSize = buffSize == null ? 4096 : buffSize;
    }

    @Test
    public void testDataIntegrityWithBufferedReader() throws URISyntaxException, IOException {
        File testFileIso = createFile(folder.newFile(), data);
        reversedLinesFileReader = new ReversedLinesFileReader(testFileIso, buffSize, encoding);

        Stack<String> lineStack = new Stack<String>();

        bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(testFileIso), encoding));
        String line;

        // read all lines in normal order
        while((line = bufferedReader.readLine())!=null) {
            lineStack.push(line);
        }

        // read in reverse order and compare with lines from stack
        while((line = reversedLinesFileReader.readLine())!=null) {
            String lineFromBufferedReader = lineStack.pop();
            assertEquals(lineFromBufferedReader, line);
        }

    }

    @After
    public void closeReader() {
        try {
            bufferedReader.close();
        } catch(Exception e) {
            // ignore
        }
        try {
            reversedLinesFileReader.close();
        } catch(Exception e) {
            // ignore
        }
    }

}
