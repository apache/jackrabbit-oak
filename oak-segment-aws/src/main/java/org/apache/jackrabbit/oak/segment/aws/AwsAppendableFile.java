/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.jackrabbit.oak.segment.aws;

import static org.apache.jackrabbit.oak.segment.aws.AwsContext.TABLE_ATTR_CONTENT;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;

import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsAppendableFile {

    private static final Logger log = LoggerFactory.getLogger(AwsAppendableFile.class);

    private final AwsContext awsContext;
    private final String fileName;

    /**
     * Initializes a file that is backed by AWS DynamoDB documents. Every line in a
     * files that is read/written is the "{@link AwsContext#TABLE_ATTR_CONTENT}"
     * attribute in a document.
     * 
     * @param awsContext The AWS context.
     * @param fileName   The name of the file.
     */
    public AwsAppendableFile(AwsContext awsContext, String fileName) {
        this.awsContext = awsContext;
        this.fileName = fileName;
    }

    public String getName() {
        return fileName;
    }

    public JournalFileWriter openJournalWriter() {
        return new AwsFileWriter(awsContext, fileName);
    }

    public JournalFileReader openJournalReader() {
        return new AwsFileReader(awsContext, fileName);
    }

    public boolean exists() {
        try {
            return openJournalReader().readLine() != null;
        } catch (IOException e) {
            log.error("Can't check if the file exists", e);
            return false;
        }
    }

    public List<String> readLines() throws IOException {
        return awsContext.getDocumentContents(fileName);
    }

    private static class AwsFileWriter implements JournalFileWriter {
        private final AwsContext awsContext;
        private final String fileName;

        public AwsFileWriter(AwsContext awsContext, String fileName) {
            this.awsContext = awsContext;
            this.fileName = fileName;
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public void truncate() throws IOException {
            awsContext.deleteAllDocuments(fileName);
        }

        @Override
        public void writeLine(String line) throws IOException {
            awsContext.putDocument(fileName, line);
        }
    }

    private static class AwsFileReader implements JournalFileReader {

        private final AwsContext awsContext;
        private final String fileName;

        private Iterator<Item> iterator;

        public AwsFileReader(AwsContext awsContext, String fileName) {
            this.awsContext = awsContext;
            this.fileName = fileName;
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public String readLine() throws IOException {
            if (iterator == null) {
                iterator = awsContext.getDocumentsStream(fileName).iterator();
            }

            if (iterator.hasNext()) {
                return iterator.next().getString(TABLE_ATTR_CONTENT);
            }

            return null;
        }
    }
}
