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

package org.apache.jackrabbit.oak.plugins.tika;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVFileGenerator {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private File outFile;

    public CSVFileGenerator(File outFile) {
        this.outFile = outFile;
    }

    public void generate(FluentIterable<BinaryResource> binaries) throws IOException {
        Closer closer = Closer.create();
        int count = 0;
        try{
            CSVPrinter printer = new CSVPrinter(Files.newWriter(outFile, Charsets.UTF_8),
                    CSVFileBinaryResourceProvider.FORMAT);
            closer.register(printer);
            for (BinaryResource br : binaries){
                count++;
                printer.printRecord(
                        br.getBlobId(),
                        br.getByteSource().size(),
                        br.getMimeType(),
                        br.getEncoding(),
                        br.getPath()
                );
                if (count % 1000 == 0) {
                    log.info("Processed {} binaries so far", count);
                }
            }
            printer.flush();
            log.info("Generated csv output at {} with {} entries", outFile.getAbsolutePath(), count);
        }finally {
            closer.close();
        }
    }
}
