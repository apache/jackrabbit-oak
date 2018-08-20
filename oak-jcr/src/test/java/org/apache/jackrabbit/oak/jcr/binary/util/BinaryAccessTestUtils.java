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
package org.apache.jackrabbit.oak.jcr.binary.util;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.jetbrains.annotations.Nullable;

/** Utility methods to test Binary direct HTTP access */
public class BinaryAccessTestUtils {

    /** Creates an nt:file based on the content, saves the session and retrieves the Binary value again. */
    public static Binary storeBinaryAndRetrieve(Session session, String path, Content content) throws RepositoryException {
        Binary binary = session.getValueFactory().createBinary(content.getStream());
        storeBinary(session, path, binary);

        return getBinary(session, path);
    }

    /** Creates an nt:file with a binary at the given path and saves the session. */
    public static void storeBinary(Session session, String path, Binary binary) throws RepositoryException {
        putBinary(session, path, binary);
        session.save();
    }

    /** Creates an nt:file with a binary at the given path. Does not save the session. */
    public static void putBinary(Session session, String ntFilePath, Binary binary) throws RepositoryException {
        Node ntResource;
        if (session.nodeExists(ntFilePath + "/" + JcrConstants.JCR_CONTENT)) {
            ntResource = session.getNode(ntFilePath + "/" + JcrConstants.JCR_CONTENT);
        } else {
            Node file = session.getRootNode().addNode(ntFilePath.substring(1), JcrConstants.NT_FILE);
            ntResource = file.addNode(JcrConstants.JCR_CONTENT, JcrConstants.NT_RESOURCE);
        }

        ntResource.setProperty(JcrConstants.JCR_DATA, binary);
    }

    /** Retrieves the Binary from the jcr:data of an nt:file */
    public static Binary getBinary(Session session, String ntFilePath) throws RepositoryException {
        return session.getNode(ntFilePath)
                .getNode(JcrConstants.JCR_CONTENT)
                .getProperty(JcrConstants.JCR_DATA)
                .getBinary();
    }

    /**
     * Uploads data via HTTP put to the provided URI.
     *
     * @param uri The URI to upload to.
     * @param contentLength Value to set in the Content-Length header.
     * @param in - The input stream to upload.
     * @return HTTP response code from the upload request.  Note that a successful
     * response for S3 is 200 - OK whereas for Azure it is 201 - Created.
     * @throws IOException
     */
    public static int httpPut(@Nullable URI uri, long contentLength, InputStream in) throws IOException  {
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

    public static boolean isSuccessfulHttpPut(int code, ConfigurableDataRecordAccessProvider dataStore) {
        if (dataStore instanceof S3DataStore) {
            return 200 == code;
        }
        else if (dataStore instanceof AzureDataStore) {
            return 201 == code;
        }
        return 200 == code;
    }

    public static boolean isFailedHttpPut(int code) {
        return code >= 400 && code < 500;
    }

    public static InputStream httpGet(@Nullable URI uri) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull("HTTP download URI is null", uri);

        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        return conn.getInputStream();
    }
}
