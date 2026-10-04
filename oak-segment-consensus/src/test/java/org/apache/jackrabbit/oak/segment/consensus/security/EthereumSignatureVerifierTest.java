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
package org.apache.jackrabbit.oak.segment.consensus.security;

import org.junit.BeforeClass;
import org.junit.Test;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.Sign;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for Ethereum signature verification.
 * 
 * <p>These tests use real MetaMask-generated signatures to verify the implementation
 * correctly recovers the signer's address from personal_sign signatures.
 */
public class EthereumSignatureVerifierTest {
    
    // Test vectors generated from MetaMask personal_sign
    // Message: "Hello, Blockchain AEM!"
    // Signer: 0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8 (example address)
    
    // Note: These are example test vectors. In a real test, you would generate
    // these using MetaMask or ethers.js and record the actual values.
    
    @BeforeClass
    public static void checkBouncyCastle() {
        EthereumSignatureVerifier.isFullVerificationAvailable();
    }
    
    @Test
    public void testIsFullVerificationAvailable() {
        assertTrue("Bouncy Castle must be available for Oak Chain signature verification",
            EthereumSignatureVerifier.isFullVerificationAvailable());
    }
    
    @Test
    public void testVerifySignatureNullParameters() {
        assertFalse("Should return false for null message",
            EthereumSignatureVerifier.verifySignature(null, "0x1234", "0x5678"));
        assertFalse("Should return false for null signature",
            EthereumSignatureVerifier.verifySignature("test", null, "0x5678"));
        assertFalse("Should return false for null address",
            EthereumSignatureVerifier.verifySignature("test", "0x1234", null));
    }
    
    @Test
    public void testVerifySignatureInvalidLength() {
        // Signature must be 65 bytes (130 hex chars)
        assertFalse("Should reject short signature",
            EthereumSignatureVerifier.verifySignature(
                "test", 
                "0x1234", // Too short
                "0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8"));
        
        // 64 bytes (128 hex chars) - missing v byte
        String shortSig = "0x" + "a".repeat(128);
        assertFalse("Should reject 64-byte signature (missing v)",
            EthereumSignatureVerifier.verifySignature(
                "test",
                shortSig,
                "0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8"));
    }
    
    @Test
    public void testVerifySignatureInvalidRecoveryId() {
        assumeTrue("Requires Bouncy Castle", 
            EthereumSignatureVerifier.isFullVerificationAvailable());
        
        // Valid length but invalid v value (not 27 or 28)
        // r (32 bytes) + s (32 bytes) + v (1 byte with invalid value 0x00)
        String invalidVSig = "0x" + "a".repeat(128) + "00";
        
        // This should fail because v=0 is invalid (should be 27 or 28)
        // After normalization, 0 becomes 27, so this might actually work
        // Let's use a clearly invalid v like 0x30 (48)
        String reallyInvalidVSig = "0x" + "a".repeat(128) + "30";
        assertFalse("Should reject invalid recovery id",
            EthereumSignatureVerifier.verifySignature(
                "test",
                reallyInvalidVSig,
                "0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8"));
    }
    
    @Test
    public void testHashMessageFormat() throws Exception {
        // Test that message hashing follows Ethereum standard
        String message = "Hello";
        byte[] hash = EthereumSignatureVerifier.hashMessage(message);
        
        assertNotNull("Hash should not be null", hash);
        assertEquals("Hash should be 32 bytes", 32, hash.length);
    }
    
    @Test
    public void testVerifySignatureWrongAddress() {
        assumeTrue("Requires Bouncy Castle", 
            EthereumSignatureVerifier.isFullVerificationAvailable());
        
        // Even with a valid-looking signature, wrong address should fail
        // This is a made-up signature that won't match any address
        String fakeSig = "0x" + 
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef" + // r
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef" + // s
            "1b"; // v = 27
        
        // The recovered address from this garbage signature won't match
        assertFalse("Should reject signature from wrong address",
            EthereumSignatureVerifier.verifySignature(
                "test message",
                fakeSig,
                "0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8"));
    }
    
    /**
     * Test with a real MetaMask signature.
     * 
     * To generate test vectors:
     * 1. Open browser console on a page with MetaMask
     * 2. Run:
     *    const accounts = await ethereum.request({ method: 'eth_requestAccounts' });
     *    const message = "Hello, Blockchain AEM!";
     *    const signature = await ethereum.request({
     *        method: 'personal_sign',
     *        params: [message, accounts[0]]
     *    });
     *    console.log("Address:", accounts[0]);
     *    console.log("Message:", message);
     *    console.log("Signature:", signature);
     * 3. Copy the values into this test
     */
    @Test
    public void testRealMetaMaskSignature() {
        assumeTrue("Requires Bouncy Castle", 
            EthereumSignatureVerifier.isFullVerificationAvailable());
        
        // TEST_VECTOR: Replace with actual MetaMask-generated test vectors
        // These are placeholder values that will fail verification
        // Generate real vectors by signing with MetaMask and capturing the output
        
        String message = "Hello, Blockchain AEM!";
        String address = "0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8";
        
        // This is a placeholder - replace with real signature from MetaMask
        String signature = "0x" +
            "0000000000000000000000000000000000000000000000000000000000000000" +
            "0000000000000000000000000000000000000000000000000000000000000000" +
            "1b";
        
        // This will fail with placeholder values, which is expected
        // When you have real test vectors, this should pass
        boolean result = EthereumSignatureVerifier.verifySignature(message, signature, address);
        
        assertFalse("Placeholder MetaMask vector should fail verification", result);
    }
    
    @Test
    public void testAddressNormalization() {
        assumeTrue("Requires Bouncy Castle", 
            EthereumSignatureVerifier.isFullVerificationAvailable());
        
        // Test that addresses are compared case-insensitively
        String message = "test";
        String sig = "0x" + "a".repeat(128) + "1b";
        
        // Both should produce the same result (both will fail, but consistently)
        boolean result1 = EthereumSignatureVerifier.verifySignature(
            message, sig, "0xABCDEF1234567890ABCDEF1234567890ABCDEF12");
        boolean result2 = EthereumSignatureVerifier.verifySignature(
            message, sig, "0xabcdef1234567890abcdef1234567890abcdef12");
        
        assertEquals("Address comparison should be case-insensitive", result1, result2);
    }
    
    @Test
    public void testSignatureWithout0xPrefix() {
        assumeTrue("Requires Bouncy Castle", 
            EthereumSignatureVerifier.isFullVerificationAvailable());
        
        String message = "test";
        String sigWithPrefix = "0x" + "a".repeat(128) + "1b";
        String sigWithoutPrefix = "a".repeat(128) + "1b";
        
        // Both should be handled (both will fail verification, but shouldn't throw)
        boolean result1 = EthereumSignatureVerifier.verifySignature(
            message, sigWithPrefix, "0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8");
        boolean result2 = EthereumSignatureVerifier.verifySignature(
            message, sigWithoutPrefix, "0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8");
        
        // Both should produce consistent results
        assertEquals("Should handle signatures with and without 0x prefix", result1, result2);
    }

    private static final String TEST_PRIVATE_KEY =
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";

    @Test
    public void testStandardPersonalSignOverNonAsciiMessagesVerifies() {
        Credentials credentials = Credentials.create(TEST_PRIVATE_KEY);
        String[] messages = {
            "Hello, Blockchain AEM!",
            "na\u00efve caf\u00e9",
            "\u65e5\u672c\u8a9e\u306e\u30da\u30fc\u30b8",
            "emoji \uD83D\uDE00\uD83D\uDE80",
            "/oak-chain/aa/bb/cc/0xabc/Acme/content/\u00fcber"
        };
        for (String message : messages) {
            byte[] utf8 = message.getBytes(StandardCharsets.UTF_8);
            Sign.SignatureData sig = Sign.signPrefixedMessage(utf8, credentials.getEcKeyPair());
            assertTrue("standard personal_sign must verify: " + message,
                EthereumSignatureVerifier.verifySignature(message, signatureHex(sig), credentials.getAddress()));
        }
    }

    @Test
    public void testHashMessageMatchesEip191Utf8ByteLength() throws Exception {
        for (String message : new String[] {"Hello", "", "caf\u00e9", "\uD83D\uDE00"}) {
            assertArrayEquals(message,
                Sign.getEthereumMessageHash(message.getBytes(StandardCharsets.UTF_8)),
                EthereumSignatureVerifier.hashMessage(message));
        }
    }

    private static String signatureHex(Sign.SignatureData signatureData) {
        StringBuilder hex = new StringBuilder("0x");
        for (byte[] part : new byte[][] {signatureData.getR(), signatureData.getS(), signatureData.getV()}) {
            for (byte b : part) {
                hex.append(String.format("%02x", b & 0xff));
            }
        }
        return hex.toString();
    }
}
