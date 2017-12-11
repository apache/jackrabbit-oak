package org.apache.jackrabbit.oak.spi.blob;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class DataStoreReference {
    private static Logger LOG = LoggerFactory.getLogger(DataStoreReference.class);

    public static DataIdentifier getIdentifierFromReference(String reference) {
        if (reference != null) {
            int colon = reference.indexOf(':');
            if (colon != -1) {
                return new DataIdentifier(reference.substring(0, colon));
            }
        }
        return null;
    }

    private static final String ALGORITHM = "HmacSHA1";
    public static String getReferenceFromIdentifier(DataIdentifier identifier,
                                                    byte[] referenceKey) {
        try {
            String id = identifier.toString();

            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(referenceKey, ALGORITHM));
            byte[] hash = mac.doFinal(id.getBytes("UTF-8"));

            return id + ':' + encodeHexString(hash);
        } catch (Exception e) {
            LOG.error("Failed to hash identifier using MAC (Message Authentication Code) algorithm.", e);
        }
        return null;
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static String encodeHexString(byte[] value) {
        char[] buffer = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            buffer[2 * i] = HEX[(value[i] >> 4) & 0x0f];
            buffer[2 * i + 1] = HEX[value[i] & 0x0f];
        }
        return new String(buffer);
    }
}
