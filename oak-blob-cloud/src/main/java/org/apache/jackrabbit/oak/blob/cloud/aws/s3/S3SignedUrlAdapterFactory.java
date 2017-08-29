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


package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import org.apache.commons.codec.binary.Base64;
import org.apache.felix.scr.annotations.*;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.value.OakValue;
import org.apache.jackrabbit.oak.spi.adapter.AdapterFactory;
import org.apache.jackrabbit.oak.spi.adapter.AdapterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;


/**
 * Adapts from Value to URI where Value has a Binary value.
 * If running as an OSGi Component would expect an OSGi AdapterManager to pick it up.
 * other
 */
@Component(immediate = true, metatype = true)
@Service(AdapterFactory.class)
public class S3SignedUrlAdapterFactory implements AdapterFactory {
    private static final String[] TARGET_CLASSES = new String[]{URI.class.getName()};

    @Property
    public static final String CLOUD_FRONT_URL = "cloudFrontUrl";
    @Property(intValue = 60)
    public static final String TTL = "ttl";
    @Property
    public static final String PRIVATE_KEY = "privateKey";
    @Property
    public static final String KEY_PAIR_ID = "keyPairId";
    public static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\n";
    public static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SignedUrlAdapterFactory.class);
    private AdapterManager adapterManager;
    private String cloudFrontUrl;
    private long ttl;
    private String keyPairId;
    private RSAPrivateKey privateKey;

    /**
     * Default Constructor used by OSGi.
     */
    public S3SignedUrlAdapterFactory() {
    }

    /**
     * Non OSGi IoC constructor, close must be called when done.
     * @param adapterManager
     * @param cloudFrontUrl
     * @param ttl
     * @param privateKeyPEM
     * @param privateKeyId
     * @throws InvalidKeySpecException
     * @throws NoSuchAlgorithmException
     */
    public S3SignedUrlAdapterFactory(AdapterManager adapterManager,
                                     String cloudFrontUrl,
                                     long ttl,
                                     String privateKeyPEM,
                                     String privateKeyId) throws InvalidKeySpecException, NoSuchAlgorithmException {
        this.adapterManager = adapterManager;
        this.adapterManager.removeAdapterFactory(this);
        init(cloudFrontUrl, ttl, privateKeyPEM, privateKeyId);
    }

    public void close() {
        deactivate(new HashMap<String, Object>());
        if ( adapterManager != null) {
            adapterManager.addAdapterFactory(this);
        }
    }

    @Deactivate
    public void deactivate(Map<String, Object> properties) {
    }


    @Activate
    public void activate(Map<String, Object> properties) throws InvalidKeySpecException, NoSuchAlgorithmException {
        init((String) properties.get(CLOUD_FRONT_URL),
                Long.parseLong((String) properties.get(TTL)),
                (String) properties.get(PRIVATE_KEY),
                (String) properties.get(KEY_PAIR_ID));
    }

    private void init(String cloudFrontUrl, long ttl, String privateKeyPEM, String privateKeyId) throws InvalidKeySpecException, NoSuchAlgorithmException {
        this.cloudFrontUrl = cloudFrontUrl;
        this.ttl = ttl;
        this.privateKey = getPrivateKey(privateKeyPEM);
        this.keyPairId = privateKeyId;
    }


    @Override
    public <T> T adaptTo(Object source, Class<T> targetClass) {
        String u = null;
        // The conversion is javax.jcr.Value -> URI, but for Oak all Values are OakValues and we can only do it for Values.
        if ( source instanceof OakValue && targetClass.equals(URI.class) ) {
            try {
                Blob b = ((OakValue) source).getBlob();
                if (b != null) {
                    String contentId = b.getContentIdentity();
                    if ( contentId != null ) {
                        // could get the cloudFrontUrl, keyParId and private key based on the resource
                        // so that multiple S3 stores or even multiple CDNs could be used, but for this PoC keeping it simple.
                        return (T) new URI(signS3Url(contentId, ttl, cloudFrontUrl, keyPairId, privateKey));
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Unable to get or sign content identity",e);
            }

        }
        return null;
    }

    @Override
    public String[] getTargetClasses() {
        return TARGET_CLASSES;
    }

    @Override
    public int getPriority() {
        return 0;
    }


    /**
     * Convert the content identiry to an S3 url, see the oak blob-cloud code for how this is done.
     * See S3Backend class in oak blob cloud package.
     * @param contentIdentity
     * @return
     */
    @Nonnull
    private String getS3Key(@Nonnull  String contentIdentity) {
        return contentIdentity.substring(0, 4) + "-" + contentIdentity.substring(4);
    }

    @Nonnull
    private String signS3Url(@Nonnull String contentIdentity, long ttl, @Nonnull String cloudFrontUrl,
                             @Nonnull String keyPairId, @Nonnull RSAPrivateKey privateKey) throws InvalidKeySpecException, NoSuchAlgorithmException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
        long expiry = (System.currentTimeMillis()/1000)+ttl;

        String urlToSign =  cloudFrontUrl+getS3Key(contentIdentity)+"?Expires="+expiry;
        StringBuilder toSign = new StringBuilder();
        toSign.append("{Statement\":[{ \"Resource\":\"")
                .append(urlToSign)
                .append("\",\"Condition\":{\"DateLessThan\":{\"AWS:EpochTime\":")
                .append(expiry).append("}}}]}");

        String signature = generateSignature(toSign, privateKey);

        toSign.append("&Signature=")
                .append(URLEncoder.encode(signature,"utf8"))
                .append("&Key-Pair-Id=")
                .append(URLEncoder.encode(keyPairId,"utf8"));
        return toSign.toString();

    }

    /**
     * This signature method assumes the private key is stored as a PEM PKCS8 encoded property on the resource.
     * For containers that have their own private key management, this will be different.
     * @param toSign
     * @param privateKey
     * @return
     */
    @Nonnull
    private String generateSignature(@Nonnull StringBuilder toSign, @Nonnull  RSAPrivateKey privateKey)
            throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
        // AWS has slightly odd replacements.
        return signBase64(toSign.toString(), privateKey).replace('+','-').replace('=','_').replace('/','~');
    }

    public String signBase64(String toSign, RSAPrivateKey privKey) throws SignatureException, InvalidKeyException, NoSuchAlgorithmException {
        Signature rsa = Signature.getInstance("SHA1withRSA");
        rsa.initSign(privKey);
        rsa.update(toSign.toString().getBytes());
        return Base64.encodeBase64String(rsa.sign());

    }


    public RSAPrivateKey getPrivateKey(String privateKeyPKCS8) throws NoSuchAlgorithmException, InvalidKeySpecException {
        int is = privateKeyPKCS8.indexOf(BEGIN_PRIVATE_KEY);
        int ie = privateKeyPKCS8.indexOf(END_PRIVATE_KEY);
        if (ie < 0 || is < 0) {
            throw new IllegalArgumentException("Private Key is not correctly encoded, need a PEM encoded key with " +
                    "-----BEGIN PRIVATE KEY----- headers to indicate PKCS8 encoding.");
        }
        privateKeyPKCS8 = privateKeyPKCS8.substring(is+BEGIN_PRIVATE_KEY.length(),ie).trim();
        byte[] privateKeyBytes = Base64.decodeBase64(privateKeyPKCS8);

        // load the private key
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        KeySpec ks = new PKCS8EncodedKeySpec(privateKeyBytes);
        return (RSAPrivateKey) keyFactory.generatePrivate(ks);

    }

}
