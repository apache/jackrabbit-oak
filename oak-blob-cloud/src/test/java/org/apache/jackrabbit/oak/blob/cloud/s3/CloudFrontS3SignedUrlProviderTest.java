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


package org.apache.jackrabbit.oak.blob.cloud.s3;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.value.OakValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

/**
 * Tests Signing URLS with a 1024 key and a 4096 key.
 */
public class CloudFrontS3SignedUrlProviderTest {


    /**
     * To generate keys in PKCS8 format use OpenSSL
     * openssl genrsa -out private_key.pem 1024
     * openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in private_key.pem -out private_key.pkcs8
     */

    private static final String PRIVATE_KEY_4096 =

    "-----BEGIN PRIVATE KEY-----\n"+
    "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\n"+
    "UzLAsK9qu+BccvxEnDDUQIxMjGUa1w==\n"+
    "-----END PRIVATE KEY-----\n";

    private static final String PRIVATE_KEY_1024 =

         "-----BEGIN PRIVATE KEY-----\n" +
        "MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAJsc0nqu+4mRGu2P\n" +
        "E39fD8awAwYRS4HyIujJaVKqXNgVTBd8R8ocSaMYPBYURQMuhKEyzc197pAxEDN8\n" +
        "g+d0pnRXS5YFUspPCehH6vsXaJSF1tW9T9J4aGSvQU+DD4G6UP9GciD2V9RRlrns\n" +
        "1K8VzJn4YvWnym5nIH/2ld1HnK8DAgMBAAECgYBCzrgtFR7L0o65kO1B5+zAk+DJ\n" +
        "hTtzXZoBj3Zon23bt9iNKP82otZog7ClhddvlmKTl3Eit8G/oxIqSs/5KbnjKFn2\n" +
        "JzHgltA64t4E+g2wfjfX/THGgq4Gf2qLABJ8XPzl4lhNhBjcp/yls+R/aaXO6Tpg\n" +
        "ot7PdSstlCJH4iHJSQJBAMzHlSXPysV4K8goh8FC392Nif4u42sMulS3yh/8hWoZ\n" +
        "zGftc/1Av9HxLYYPI7VojGVl8ZEi7mWWFpYhnjGi4MUCQQDB6Pe603RFg8EPX49Z\n" +
        "3BNuxk4/17znWktbhCzo/MxOGZ/xoCqEIO59jIUYgrWzp4PD7tLd8uPw8CKyV6RA\n" +
        "H70nAkEAtxg4Rd+dmKQS2VtLzkOh7/FOkYOQ+TUfIQJXafQZwb0hY0cPbz/GgssW\n" +
        "fgfpUJkS9tKIg9FswE3LEh8q14yfVQJBAJhDG8tq4QD4zS8KJWE1K1kvhCAjgmTU\n" +
        "BRpOdolYgADPpEUyNFBeH4ccQSKW+KCiaBgBPzki0ZPOrCAIK6Rih78CQQDIICap\n" +
        "QByVjsFDed4Te13qthgFthy6iGyk1JZFu9lCnAaiAdg4AA0OyF9FxFUzCsOGD1HS\n" +
        "GoRuVX4I/AZR74Sx \n" +
        "-----END PRIVATE KEY-----";
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudFrontS3SignedUrlProviderTest.class);
    @Mock
    private Blob blob;

    interface MockJCRValue extends Value, OakValue {
    }

    @Mock
    private MockJCRValue value;


    public CloudFrontS3SignedUrlProviderTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSignedURL() throws InvalidKeySpecException, NoSuchAlgorithmException, RepositoryException {
        long t2 = System.currentTimeMillis();
        CloudFrontS3SignedUrlProvider provider = new CloudFrontS3SignedUrlProvider(
                "http://applicationA1.cloudfront.net/",
                60,
                1,
                PRIVATE_KEY_1024,
        "123");
        LOGGER.info("Loaded 1024 private key in {} ms "+(System.currentTimeMillis()-t2));

        long t = System.currentTimeMillis();
        CloudFrontS3SignedUrlProvider provider4096 = new CloudFrontS3SignedUrlProvider(
                "http://applicationA1.cloudfront.net/",
                60,
                1,
                PRIVATE_KEY_4096,
                "PRIVATE_KEY_4096");
        LOGGER.info("Loaded 4096 private key in {} ms "+(System.currentTimeMillis()-t));



        t = System.currentTimeMillis();
        Mockito.when(blob.length()).thenReturn((long)1024*1024); // 1MB
        Mockito.when(value.getBlob()).thenReturn(blob);
        Mockito.when(blob.getContentIdentity()).thenReturn("1234567891ABCDEFGH");
        URI u = provider4096.toURI(value);
        Assert.assertNotNull(u);
        LOGGER.info("Signed with 4096 key in {} ms ",(System.currentTimeMillis()-t));

        t = System.currentTimeMillis();
        Mockito.when(blob.length()).thenReturn((long)1024*1024); // 1MB
        Mockito.when(value.getBlob()).thenReturn(blob);
        Mockito.when(blob.getContentIdentity()).thenReturn("1234567891ABCDEFGH");
        u = provider.toURI(value);
        Assert.assertNotNull(u);
        LOGGER.info("Signed with 1024 key in {} ms ",(System.currentTimeMillis()-t));
    }
}
