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
package org.apache.jackrabbit.oak.segment.aws;

import com.amazonaws.services.dynamodbv2.document.Item;

import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.apache.jackrabbit.oak.segment.aws.DynamoDBClient.TABLE_ATTR_CONTENT;

public class AwsJournalFile implements JournalFile {

    private static final Logger log = LoggerFactory.getLogger(AwsJournalFile.class);

    private final DynamoDBClient dynamoDBClient;
    private final String fileName;

    public AwsJournalFile(DynamoDBClient dynamoDBClient, String fileName) {
        this.dynamoDBClient = dynamoDBClient;
        this.fileName = fileName;
    }

    @Override
    public JournalFileReader openJournalReader() throws IOException {
        return new AwsFileReader(dynamoDBClient, fileName);
    }

    @Override
    public JournalFileWriter openJournalWriter() throws IOException {
        return new AwsFileWriter(dynamoDBClient, fileName);
    }

    @Override
    public String getName() {
        return fileName;
    }

    @Override
    public boolean exists() {
        try {
            return openJournalReader().readLine() != null;
        } catch (IOException e) {
            log.error("Can't check if the file exists", e);
            return false;
        }
    }

    private static class AwsFileWriter implements JournalFileWriter {
        private final DynamoDBClient dynamoDBClient;
        private final String fileName;

        public AwsFileWriter(DynamoDBClient dynamoDBClient, String fileName) {
            this.dynamoDBClient = dynamoDBClient;
            this.fileName = fileName;
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public void truncate() throws IOException {
            dynamoDBClient.deleteAllDocuments(fileName);
        }

        @Override
        public void writeLine(String line) throws IOException {
            dynamoDBClient.putDocument(fileName, line);
        }

        @Override
        public void batchWriteLines(List<String> lines) throws IOException {
            dynamoDBClient.batchPutDocument(fileName, lines);
        }
    }

    private static class AwsFileReader implements JournalFileReader {

        private final DynamoDBClient dynamoDBClient;
        private final String fileName;

        private Iterator<Item> iterator;

        public AwsFileReader(DynamoDBClient dynamoDBClient, String fileName) {
            this.dynamoDBClient = dynamoDBClient;
            this.fileName = fileName;
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public String readLine() throws IOException {
            if (iterator == null) {
                iterator = dynamoDBClient.getDocumentsStream(fileName).iterator();
            }

            if (iterator.hasNext()) {
                return iterator.next().getString(TABLE_ATTR_CONTENT);
            }

            return null;
        }
    }
}