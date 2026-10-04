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

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Security;
import java.util.Arrays;

/**
 * Ethereum signature verifier for MetaMask personal_sign signatures.
 * 
 * <p>Verifies signatures created by MetaMask's personal_sign method, which uses
 * the Ethereum standard message prefix and secp256k1 ECDSA.
 * 
 * <p>Signature format (65 bytes):
 * <ul>
 *   <li>r: 32 bytes (signature component)</li>
 *   <li>s: 32 bytes (signature component)</li>
 *   <li>v: 1 byte (recovery id, 27 or 28)</li>
 * </ul>
 * 
 * <p>Message format (Ethereum personal_sign):
 * <pre>
 * "\x19Ethereum Signed Message:\n" + utf8ByteLength(message) + message
 * </pre>
 * 
 * @see <a href="https://eips.ethereum.org/EIPS/eip-191">EIP-191: Signed Data Standard</a>
 */
public class EthereumSignatureVerifier {
    
    private static final Logger log = LoggerFactory.getLogger(EthereumSignatureVerifier.class);
    
    // Ethereum message prefix for personal_sign
    private static final String ETHEREUM_MESSAGE_PREFIX = "\u0019Ethereum Signed Message:\n";
    
    // secp256k1 curve parameters
    private static final BigInteger SECP256K1_N = new BigInteger(
        "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16);
    private static final BigInteger SECP256K1_HALF_N = SECP256K1_N.shiftRight(1);
    
    // Bouncy Castle provider name
    private static final String BC_PROVIDER = "BC";
    
    // Flag to track if Bouncy Castle is available
    private static boolean bouncyCastleAvailable = false;
    private static String availabilityReason = "Bouncy Castle provider not initialized";
    
    static {
        try {
            if (Security.getProvider(BC_PROVIDER) == null) {
                Security.addProvider(new BouncyCastleProvider());
            }
            bouncyCastleAvailable = true;
            availabilityReason = null;
            log.info("✅ Bouncy Castle provider loaded for secp256k1 signature verification");
        } catch (Throwable t) {
            availabilityReason = "Bouncy Castle provider unavailable: " + t.getMessage();
            log.error("❌ {}", availabilityReason, t);
            bouncyCastleAvailable = false;
        }
    }
    
    /**
     * Verify an Ethereum personal_sign signature.
     * 
     * @param message The original message that was signed
     * @param signatureHex The signature in hex format (0x + 130 hex chars = 65 bytes)
     * @param expectedAddress The expected signer's Ethereum address (0x + 40 hex chars)
     * @return true if signature is valid and signer matches expected address
     */
    public static boolean verifySignature(String message, String signatureHex, String expectedAddress) {
        if (message == null || signatureHex == null || expectedAddress == null) {
            log.warn("❌ Signature verification failed: null parameter");
            return false;
        }

        if (!bouncyCastleAvailable) {
            log.error("❌ Full Ethereum signature verification unavailable: {}", getAvailabilityReason());
            return false;
        }
        
        try {
            // Normalize inputs
            String normalizedSig = signatureHex.toLowerCase().startsWith("0x") 
                ? signatureHex.substring(2) : signatureHex;
            String normalizedAddress = expectedAddress.toLowerCase().startsWith("0x")
                ? expectedAddress.substring(2).toLowerCase() : expectedAddress.toLowerCase();
            
            // Validate signature length (65 bytes = 130 hex chars)
            if (normalizedSig.length() != 130) {
                log.warn("❌ Invalid signature length: {} (expected 130 hex chars)", normalizedSig.length());
                return false;
            }
            
            // Parse signature components (r, s, v)
            byte[] signatureBytes = hexToBytes(normalizedSig);
            byte[] r = Arrays.copyOfRange(signatureBytes, 0, 32);
            byte[] s = Arrays.copyOfRange(signatureBytes, 32, 64);
            int v = signatureBytes[64] & 0xFF;
            
            // Normalize v (MetaMask uses 27/28, some use 0/1)
            if (v < 27) {
                v += 27;
            }
            if (v != 27 && v != 28) {
                log.warn("❌ Invalid recovery id v={}", v);
                return false;
            }
            
            // Hash the message with Ethereum prefix
            byte[] messageHash = hashMessage(message);
            
            // Recover public key from signature
            byte[] recoveredPubKey = recoverPublicKey(messageHash, r, s, v - 27);
            if (recoveredPubKey == null) {
                log.warn("❌ Failed to recover public key from signature");
                return false;
            }
            
            // Derive address from public key
            String recoveredAddress = publicKeyToAddress(recoveredPubKey);
            
            // Compare addresses (case-insensitive)
            boolean matches = recoveredAddress.equalsIgnoreCase(normalizedAddress);
            
            if (matches) {
                log.debug("✅ Signature verified: recovered address {} matches expected {}", 
                    recoveredAddress, normalizedAddress);
            } else {
                log.warn("❌ Signature mismatch: recovered {} but expected {}", 
                    recoveredAddress, normalizedAddress);
            }
            
            return matches;
            
        } catch (Exception e) {
            log.error("❌ Signature verification error", e);
            return false;
        }
    }
    
    /**
     * Hash a message using Ethereum's personal_sign format.
     * 
     * @param message The message to hash
     * @return Keccak-256 hash of the prefixed message
     */
    public static byte[] hashMessage(String message) throws Exception {
        int utf8Length = message.getBytes(StandardCharsets.UTF_8).length;
        String prefixedMessage = ETHEREUM_MESSAGE_PREFIX + utf8Length + message;
        return keccak256(prefixedMessage.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Recover the public key from an ECDSA signature.
     * 
     * @param messageHash The hash of the signed message
     * @param r Signature component r (32 bytes)
     * @param s Signature component s (32 bytes)
     * @param recoveryId Recovery id (0 or 1)
     * @return The recovered public key (64 bytes, uncompressed without 0x04 prefix)
     */
    private static byte[] recoverPublicKey(byte[] messageHash, byte[] r, byte[] s, int recoveryId) {
        requireFullVerificationAvailable();
        
        try {
            BigInteger rBigInt = new BigInteger(1, r);
            BigInteger sBigInt = new BigInteger(1, s);
            
            // Validate s is in lower half of curve order (EIP-2)
            if (sBigInt.compareTo(SECP256K1_HALF_N) > 0) {
                log.debug("Normalizing s value (was in upper half)");
                sBigInt = SECP256K1_N.subtract(sBigInt);
            }
            
            // Use Bouncy Castle for EC recovery
            org.bouncycastle.crypto.params.ECDomainParameters ecParams = getSecp256k1Params();
            org.bouncycastle.math.ec.ECPoint R = recoverPoint(ecParams, rBigInt, recoveryId);
            
            if (R == null) {
                return null;
            }
            
            BigInteger e = new BigInteger(1, messageHash);
            BigInteger n = ecParams.getN();
            BigInteger eInv = e.negate().mod(n);
            BigInteger rInv = rBigInt.modInverse(n);
            
            // Q = r^-1 * (s*R - e*G)
            org.bouncycastle.math.ec.ECPoint G = ecParams.getG();
            org.bouncycastle.math.ec.ECPoint sR = R.multiply(sBigInt);
            org.bouncycastle.math.ec.ECPoint eG = G.multiply(eInv);
            org.bouncycastle.math.ec.ECPoint Q = sR.add(eG).multiply(rInv);
            
            // Get uncompressed public key (remove 0x04 prefix)
            byte[] pubKeyFull = Q.getEncoded(false);
            return Arrays.copyOfRange(pubKeyFull, 1, 65); // Remove 0x04 prefix
            
        } catch (Exception e) {
            log.error("Failed to recover public key", e);
            return null;
        }
    }
    
    /**
     * Get secp256k1 curve parameters.
     */
    private static org.bouncycastle.crypto.params.ECDomainParameters getSecp256k1Params() {
        org.bouncycastle.asn1.x9.X9ECParameters curveParams = 
            org.bouncycastle.crypto.ec.CustomNamedCurves.getByName("secp256k1");
        return new org.bouncycastle.crypto.params.ECDomainParameters(
            curveParams.getCurve(),
            curveParams.getG(),
            curveParams.getN(),
            curveParams.getH()
        );
    }
    
    /**
     * Recover EC point R from signature component r and recovery id.
     */
    private static org.bouncycastle.math.ec.ECPoint recoverPoint(
            org.bouncycastle.crypto.params.ECDomainParameters ecParams,
            BigInteger r, int recoveryId) {
        
        BigInteger n = ecParams.getN();
        BigInteger i = BigInteger.valueOf(recoveryId / 2);
        BigInteger x = r.add(i.multiply(n));
        
        // Check x is valid
        BigInteger prime = ecParams.getCurve().getField().getCharacteristic();
        if (x.compareTo(prime) >= 0) {
            return null;
        }

        // Decompress point
        byte[] compressedPoint = new byte[33];
        compressedPoint[0] = (byte) ((recoveryId & 1) == 0 ? 0x02 : 0x03);
        byte[] xBytes = x.toByteArray();
        
        // Handle leading zero or padding
        if (xBytes.length == 33) {
            System.arraycopy(xBytes, 1, compressedPoint, 1, 32);
        } else if (xBytes.length < 32) {
            System.arraycopy(xBytes, 0, compressedPoint, 33 - xBytes.length, xBytes.length);
        } else {
            System.arraycopy(xBytes, 0, compressedPoint, 1, 32);
        }
        
        try {
            return ecParams.getCurve().decodePoint(compressedPoint);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Derive Ethereum address from public key.
     * 
     * @param publicKey 64-byte uncompressed public key (without 0x04 prefix)
     * @return Ethereum address (40 hex chars, no 0x prefix)
     */
    private static String publicKeyToAddress(byte[] publicKey) throws Exception {
        byte[] hash = keccak256(publicKey);
        // Take last 20 bytes
        byte[] addressBytes = Arrays.copyOfRange(hash, 12, 32);
        return bytesToHex(addressBytes);
    }
    
    /**
     * Compute Keccak-256 hash.
     */
    private static byte[] keccak256(byte[] input) throws Exception {
        requireFullVerificationAvailable();
        org.bouncycastle.crypto.digests.KeccakDigest digest =
            new org.bouncycastle.crypto.digests.KeccakDigest(256);
        digest.update(input, 0, input.length);
        byte[] hash = new byte[32];
        digest.doFinal(hash, 0);
        return hash;
    }
    
    /**
     * Check if full Ethereum signature verification is available.
     * 
     * @return true if Bouncy Castle is loaded and secp256k1 verification works
     */
    public static boolean isFullVerificationAvailable() {
        return bouncyCastleAvailable;
    }

    /**
     * Fail closed when Ethereum verification dependencies are unavailable.
     */
    public static void requireFullVerificationAvailable() {
        if (!bouncyCastleAvailable) {
            throw new IllegalStateException(getAvailabilityReason());
        }
    }

    /**
     * Describe why full verification is unavailable.
     */
    public static String getAvailabilityReason() {
        return availabilityReason != null ? availabilityReason : "Bouncy Castle provider is available";
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Hex Utilities
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    private static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i+1), 16));
        }
        return data;
    }
}
