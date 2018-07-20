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
package org.apache.jackrabbit.oak.jcr.binary;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordDirectAccessProvider;
import org.jetbrains.annotations.Nullable;

public class BinaryAccessTestUtils {
    static final int MB = 1024 * 1024;
    static final int SECONDS = 1000;

    static Node getOrCreateNtFile(Session session, String path) throws RepositoryException {
        if (session.nodeExists(path + "/" + JcrConstants.JCR_CONTENT)) {
            return session.getNode(path + "/" + JcrConstants.JCR_CONTENT);
        }
        Node file = session.getRootNode().addNode(path.substring(1), JcrConstants.NT_FILE);
        return file.addNode(JcrConstants.JCR_CONTENT, JcrConstants.NT_RESOURCE);
    }

    static void putBinary(Session session, String ntFilePath, Binary binary) throws RepositoryException {
        Node node = getOrCreateNtFile(session, ntFilePath);
        node.setProperty(JcrConstants.JCR_DATA, binary);
    }

    static Binary getBinary(Session session, String ntFilePath) throws RepositoryException {
        return session.getNode(ntFilePath)
                .getNode(JcrConstants.JCR_CONTENT)
                .getProperty(JcrConstants.JCR_DATA)
                .getBinary();
    }

    static String getRandomString(long size) {
        String base = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ+/";
        StringWriter writer = new StringWriter();
        Random random = new Random();
        if (size > 256) {
            String str256 = getRandomString(256);
            while (size >= 256) {
                writer.write(str256);
                size -= 256;
            }
            if (size > 0) {
                writer.write(str256.substring(0, (int)size));
            }
        }
        else {
            for (long i = 0; i < size; i++) {
                writer.append(base.charAt(random.nextInt(base.length())));
            }
        }
        return writer.toString();
    }

    /** Saves an nt:file with a binary at the given path and saves the session. */
    static void saveFileWithBinary(Session session, String path, Binary binary) throws RepositoryException {
        Node node = getOrCreateNtFile(session, path);
        node.setProperty(JcrConstants.JCR_DATA, binary);
        session.save();
    }

    static Binary createFileWithBinary(Session session, String path, InputStream stream) throws RepositoryException {
        Binary binary = session.getValueFactory().createBinary(stream);
        saveFileWithBinary(session, path, binary);
        return binary;
    }

    static void waitForUploads() throws InterruptedException {
        // let data store upload threads finish
        Thread.sleep(5 * SECONDS);
    }

    static InputStream getTestInputStream(String content) {
        try {
            return new ByteArrayInputStream(content.getBytes("utf-8"));
        } catch (UnsupportedEncodingException unexpected) {
            unexpected.printStackTrace();
            // return empty stream
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    static InputStream getTestInputStream(int size) {
        byte[] blob = new byte[size];
        // magic bytes so it's not just all zeros
        blob[0] = 1;
        blob[1] = 2;
        blob[2] = 3;
        return new ByteArrayInputStream(blob);
    }

    static int httpPut(@Nullable URI uri, long contentLength, InputStream in) throws IOException {
        return httpPut(uri, contentLength, in, false);
    }

    /**
     * Uploads data via HTTP put to the provided URI.
     *
     * @param uri The URI to upload to.
     * @param contentLength Value to set in the Content-Length header.
     * @param in - The input stream to upload.
     * @param isMultiPart - True if this upload is part of a multi-part upload.
     * @return HTTP response code from the upload request.  Note that a successful
     * response for S3 is 200 - OK whereas for Azure it is 201 - Created.
     * @throws IOException
     */
    static int httpPut(@Nullable URI uri, long contentLength, InputStream in, boolean isMultiPart) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(uri);

        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("Content-Length", String.valueOf(contentLength));
        connection.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));

        OutputStream putStream = connection.getOutputStream();
        IOUtils.copy(in, putStream);
        putStream.close();
        return connection.getResponseCode();
    }

    static boolean isSuccessfulHttpPut(int code, ConfigurableDataRecordDirectAccessProvider dataStore) {
        if (dataStore instanceof S3DataStore) {
            return 200 == code;
        }
        else if (dataStore instanceof AzureDataStore) {
            return 201 == code;
        }
        return 200 == code;
    }

    static boolean isFailedHttpPut(int code) {
        return code >= 400 && code < 500;
    }

    static int httpPutTestStream(URI uri) throws IOException {
        String content = "hello world";
        return httpPut(uri, content.getBytes().length, getTestInputStream(content));
    }

    static InputStream httpGet(@Nullable URI uri) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(uri);

        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        return conn.getInputStream();
    }
}
