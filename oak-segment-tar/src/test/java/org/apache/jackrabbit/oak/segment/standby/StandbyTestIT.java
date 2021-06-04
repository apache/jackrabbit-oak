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

package org.apache.jackrabbit.oak.segment.standby;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Random;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class StandbyTestIT extends TestBase {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, false);

    private TemporaryFileStore clientFileStore = new TemporaryFileStore(folder, true);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
            .around(serverFileStore)
            .around(clientFileStore);

    /**
     * This test syncs a few segments over an unencrypted connection.
     */
    @Test
    public void testSync() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(false)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .build()
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, 150);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

            PropertyState ps = secondary.getHead().getChildNode("root")
                .getChildNode("server").getProperty("testBlob");
            assertNotNull(ps);
            assertEquals(Type.BINARY.tag(), ps.getType().tag());
            Blob b = ps.getValue(Type.BINARY);
            assertEquals(blobSize, b.length());

            byte[] testData = new byte[blobSize];
            ByteStreams.readFully(b.getNewStream(), testData);
            assertArrayEquals(data, testData);
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * Both server and client certificates are generated on-the-fly.
     */
    @Test
    @Ignore("This test takes ~4s and is therefore disabled by default")
    public void testSyncSSL() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .build()
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, 150);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

            PropertyState ps = secondary.getHead().getChildNode("root")
                    .getChildNode("server").getProperty("testBlob");
            assertNotNull(ps);
            assertEquals(Type.BINARY.tag(), ps.getType().tag());
            Blob b = ps.getValue(Type.BINARY);
            assertEquals(blobSize, b.length());

            byte[] testData = new byte[blobSize];
            ByteStreams.readFully(b.getNewStream(), testData);
            assertArrayEquals(data, testData);
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore.
     * The server does not validate the client certificate.
     * The client creates its certificate on-the-fly.
     */
    @Test
    @Ignore("This test takes ~2s and is therefore disabled by default")
    public void testSyncSSLNoClientValidation() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, serverKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(false)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .build()
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

            PropertyState ps = secondary.getHead().getChildNode("root")
                .getChildNode("server").getProperty("testBlob");
            assertNotNull(ps);
            assertEquals(Type.BINARY.tag(), ps.getType().tag());
            Blob b = ps.getValue(Type.BINARY);
            assertEquals(blobSize, b.length());

            byte[] testData = new byte[blobSize];
            ByteStreams.readFully(b.getNewStream(), testData);
            assertArrayEquals(data, testData);
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore.
     * The server validates the client certificate.
     * The client has a configured certificate which can be validated with the truststore.
     */
    @Test
    @Ignore("This test takes ~2s and is therefore disabled by default")
    public void testSyncSSLValidClient() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, serverKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, clientKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, clientCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .build()
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

            PropertyState ps = secondary.getHead().getChildNode("root")
                .getChildNode("server").getProperty("testBlob");
            assertNotNull(ps);
            assertEquals(Type.BINARY.tag(), ps.getType().tag());
            Blob b = ps.getValue(Type.BINARY);
            assertEquals(blobSize, b.length());

            byte[] testData = new byte[blobSize];
            ByteStreams.readFully(b.getNewStream(), testData);
            assertArrayEquals(data, testData);
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore.
     * The server validates the client certificate.
     * The client has a configured certificate which can be validated with the truststore.
     * All the keys are encrypted.
     */
    @Test
    @Ignore("This test takes ~2s and is therefore disabled by default")
    public void testSyncSSLValidClientEncryptedKeys() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, encryptedServerKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, encryptedClientKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, clientCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLKeyPassword(secretPassword)
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLKeyPassword(secretPassword)
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .build()
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

            PropertyState ps = secondary.getHead().getChildNode("root")
                .getChildNode("server").getProperty("testBlob");
            assertNotNull(ps);
            assertEquals(Type.BINARY.tag(), ps.getType().tag());
            Blob b = ps.getValue(Type.BINARY);
            assertEquals(blobSize, b.length());

            byte[] testData = new byte[blobSize];
            ByteStreams.readFully(b.getNewStream(), testData);
            assertArrayEquals(data, testData);
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore.
     * The server validates the client certificate.
     * The client has a configured certificate which cannot be validated with the truststore.
     * The SSL connection is expected to fail.
     */
    @Test
    @Ignore("This test takes ~7s and is therefore disabled by default")
    public void testSyncSSLInvalidClient() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, serverKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, arbitraryKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, arbitraryCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .build()
        ) {
            serverSync.start();
            addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertNotEquals(primary.getHead(), secondary.getHead());
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which cannot be validated with the truststore.
     * The server validates the client certificate.
     * The client has a configured certificate which can be validated with the truststore.
     * The SSL connection is expected to fail.
     */
    @Test
    @Ignore("This test takes ~7s and is therefore disabled by default")
    public void testSyncSSLInvalidServer() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, arbitraryKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, clientKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, arbitraryCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, clientCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .build()
        ) {
            serverSync.start();
            addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertNotEquals(primary.getHead(), secondary.getHead());
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore.
     * The server validates the client certificate.
     * The client has a configured certificate which can be validated with the truststore,
     * but cannot be validated against the required subject pattern.
     * The SSL connection is expected to fail.
     */
    @Test
    @Ignore("This test takes ~7s and is therefore disabled by default")
    public void testSyncSSLInvalidClientSubject() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, serverKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, clientKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, clientCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .withSSLSubjectPattern("foobar")
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .build()
        ) {
            serverSync.start();
            addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertNotEquals(primary.getHead(), secondary.getHead());
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore.
     * The server validates the client certificate.
     * The client has a configured certificate which can be validated with the truststore,
     * and can also be validated against the required subject pattern.
     */
    @Test
    @Ignore("This test takes ~7s and is therefore disabled by default")
    public void testSyncSSLValidClientSubject() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, serverKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, clientKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, clientCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .withSSLSubjectPattern(".*.esting.*")
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .build()
        ) {
            serverSync.start();
            addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore,
     * but cannot be validated against the required subject pattern.
     * The server validates the client certificate.
     * The client has a configured certificate which can be validated with the truststore.
     * The SSL connection is expected to fail.
     */
    @Test
    @Ignore("This test takes ~7s and is therefore disabled by default")
    public void testSyncSSLInvalidServerSubject() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, serverKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, clientKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, clientCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .withSSLSubjectPattern("foobar")
                .build()
        ) {
            serverSync.start();
            addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertNotEquals(primary.getHead(), secondary.getHead());
        }
    }

    /**
     * This test syncs a few segments over an encrypted connection.
     * The server has a configured certificate which can be validated with the truststore,
     * and can also be validated against the required subject pattern.
     * The server validates the client certificate.
     * The client has a configured certificate which can be validated with the truststore.
     */
    @Test
    @Ignore("This test takes ~7s and is therefore disabled by default")
    public void testSyncSSLValidServerSubject() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        FileOutputStream fos;

        File serverKeyFile = folder.newFile();
        fos = new FileOutputStream(serverKeyFile);
        IOUtils.writeString(fos, serverKey);
        fos.close();

        File clientKeyFile = folder.newFile();
        fos = new FileOutputStream(clientKeyFile);
        IOUtils.writeString(fos, clientKey);
        fos.close();

        File serverCertFile = folder.newFile();
        fos = new FileOutputStream(serverCertFile);
        IOUtils.writeString(fos, serverCert);
        fos.close();

        File clientCertFile = folder.newFile();
        fos = new FileOutputStream(clientCertFile);
        IOUtils.writeString(fos, clientCert);
        fos.close();

        File keyStoreFile = folder.newFile();
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, "changeit".toCharArray());
        Certificate c = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caCert.getBytes()));
        keyStore.setCertificateEntry("the-ca-cert", c);
        keyStore.store(new FileOutputStream(keyStoreFile), "changeit".toCharArray());
        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getAbsolutePath());

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .withSecureConnection(true)
                .withSSLKeyFile(serverKeyFile.getAbsolutePath())
                .withSSLChainFile(serverCertFile.getAbsolutePath())
                .withSSLClientValidation(true)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(true)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .withSSLKeyFile(clientKeyFile.getAbsolutePath())
                .withSSLChainFile(clientCertFile.getAbsolutePath())
                .withSSLSubjectPattern(".*.esting.*")
                .build()
        ) {
            serverSync.start();
            addTestContent(store, "server", blobSize, 1);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());
        }
    }

    /**
     * OAK-2430
     */
    @Test
    public void testSyncLoop() throws Exception {
        final int blobSize = 25 * 1024;
        final int dataNodes = 5000;

        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .build();
            StandbyClientSync clientSync = StandbyClientSync.builder()
                .withHost(getServerHost())
                .withPort(serverPort.getPort())
                .withFileStore(secondary)
                .withSecureConnection(false)
                .withReadTimeoutMs(getClientTimeout())
                .withAutoClean(false)
                .withSpoolFolder(folder.newFolder())
                .build()
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, dataNodes);
            primary.flush();

            for (int i = 0; i < 5; i++) {
                String cp = store.checkpoint(Long.MAX_VALUE);
                primary.flush();
                clientSync.run();
                assertEquals(primary.getHead(), secondary.getHead());
                assertTrue(store.release(cp));
                clientSync.cleanup();
                assertTrue(secondary.getStats().getApproximateSize() > blobSize);
            }

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

            PropertyState ps = secondary.getHead().getChildNode("root")
                    .getChildNode("server").getProperty("testBlob");
            assertNotNull(ps);
            assertEquals(Type.BINARY.tag(), ps.getType().tag());
            Blob b = ps.getValue(Type.BINARY);
            assertEquals(blobSize, b.length());

            byte[] testData = new byte[blobSize];
            ByteStreams.readFully(b.getNewStream(), testData);
            assertArrayEquals(data, testData);
        }
    }

    private static byte[] addTestContent(NodeStore store, String child, int size, int dataNodes) throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder content = builder.child(child);
        content.setProperty("ts", System.currentTimeMillis());

        byte[] data = new byte[size];
        new Random().nextBytes(data);
        Blob blob = store.createBlob(new ByteArrayInputStream(data));
        content.setProperty("testBlob", blob);

        for (int i = 0; i < dataNodes; i++) {
            NodeBuilder c = content.child("c" + i);
            for (int j = 0; j < 1000; j++) {
                c.setProperty("p" + i, "v" + i);
            }
        }

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return data;
    }

}
