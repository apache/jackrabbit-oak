package org.apache.jackrabbit.oak.blob.cloud.s3;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.AbstractURLWritableBlobStoreTest;
import org.apache.jackrabbit.oak.spi.blob.DirectBinaryAccessException;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStore;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStoreUploadContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.HttpsURLConnection;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class S3DataStoreWritableURLTest extends AbstractURLWritableBlobStoreTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static S3DataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        dataStore = (S3DataStore) S3DataStoreUtils.getS3DataStore(
                "org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore",
                getProperties("s3.config",
                        "aws.properties",
                        ".aws"),
                homeDir.newFolder().getAbsolutePath()
        );
        dataStore.setURLWritableBinaryExpirySeconds(expirySeconds);
    }

    @Override
    protected URLWritableDataStore getDataStore() {
        return dataStore;
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((S3DataStore)ds).deleteRecord(identifier);
    }

    @Override
    protected long getProviderMinPartSize() {
        return Math.max(0L, S3DataStore.minPartSize);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return ONE_MB*256;
    }

    @Override
    protected boolean isSinglePutURL(URL url) {
        Map<String, String> queryParams = parseQueryString(url);
        return ! queryParams.containsKey(S3Backend.PART_NUMBER) && ! queryParams.containsKey(S3Backend.UPLOAD_ID);
    }

    @Override
    protected boolean isValidUploadToken(String uploadToken) {
        return null != S3Backend.DirectUploadToken.fromEncodedToken(uploadToken);
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URL url) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));
        conn.setRequestProperty("Host", url.getHost());

        assertEquals("PUT", conn.getRequestMethod());
        return conn;
    }

    @Test
    public void testInitDirectUploadURLHonorsExpiryTime() throws DirectBinaryAccessException {
        URLWritableDataStore ds = getDataStore();
        try {
            ds.setURLWritableBinaryExpirySeconds(60);
            URLWritableDataStoreUploadContext context = ds.initDirectUpload(ONE_MB, 1);
            URL uploadUrl = context.getUploadPartURLs().get(0);
            Map<String, String> params = parseQueryString(uploadUrl);
            String expiresTime = params.get("X-Amz-Expires");
            assertTrue(60 >= Integer.parseInt(expiresTime));
        }
        finally {
            ds.setURLWritableBinaryExpirySeconds(expirySeconds);
        }
    }
}
