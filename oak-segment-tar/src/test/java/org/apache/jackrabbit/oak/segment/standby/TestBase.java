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

import static org.junit.Assume.assumeTrue;

import org.apache.commons.lang3.SystemUtils;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.junit.BeforeClass;

public class TestBase {

    static final int MB = 1024 * 1024;

    private static final int timeout = Integer.getInteger("standby.test.timeout", 5000);

    // Java 6 on Windows doesn't support dual IP stacks, so we will skip our
    // IPv6 tests.
    final boolean noDualStackSupport = SystemUtils.IS_OS_WINDOWS && SystemUtils.IS_JAVA_1_6;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(!CIHelper.travis());
    }

    static String getServerHost() {
        return "127.0.0.1";
    }

    static int getClientTimeout() {
        return timeout;
    }

    // A self-signed certificate representing the "Testing CA"
    static final String caCert =
        "-----BEGIN CERTIFICATE-----\n" +
            "MIIDxjCCAq4CCQDWKVsDO4p3MDANBgkqhkiG9w0BAQUFADCBpDELMAkGA1UEBhMC\n" +
            "VVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28x\n" +
            "FjAUBgNVBAoMDUNBVGVzdGluZyBDby4xEDAOBgNVBAsMB1Rlc3RpbmcxFjAUBgNV\n" +
            "BAMMDWNhdGVzdGluZy5jb20xJjAkBgkqhkiG9w0BCQEWF2NhdGVzdGluZ0BjYXRl\n" +
            "c3RpbmcuY29tMB4XDTIxMDUzMTEzNTczOFoXDTQ4MTAxNTEzNTczOFowgaQxCzAJ\n" +
            "BgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1TYW4gRnJh\n" +
            "bmNpc2NvMRYwFAYDVQQKDA1DQVRlc3RpbmcgQ28uMRAwDgYDVQQLDAdUZXN0aW5n\n" +
            "MRYwFAYDVQQDDA1jYXRlc3RpbmcuY29tMSYwJAYJKoZIhvcNAQkBFhdjYXRlc3Rp\n" +
            "bmdAY2F0ZXN0aW5nLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB\n" +
            "AKmolPe+fw1pQ91MNrlHBdRCX5J55vuV7zqbKU8UdZVuI3iajvhFtMm6xxIyISIM\n" +
            "cO+h0JYKH0NHgCV0wqUKh+T16FEPo8BskxMCXIn0R0IQBBeUIIS3+FtNzRhLK3ff\n" +
            "R7jtpv4YMp50tx8FVj9zIYFnLq5RObH7hENrxLTk04G3Gr8ENqapH21pfVlkb4GP\n" +
            "RXBonxyR/q997ZGqutxiEbsCeWDMl/uvPDL3zKbd2e/Hx0Xy5kBOqAF35JTmKxax\n" +
            "up33ld3u3r2MlFzcD7l/2uG7ur42MNDFBG2yQXsmPFUE6/Ogk/IfuGiqVYYjjRZV\n" +
            "ixO+HK3WnICJggsNDQO6CqsCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAOjhaAvUw\n" +
            "AeQ6PO485xoNpnadrFu+Q1RafqZCHd/UJ/c2VLeEogsBjZpLMMclF01T6k7X/6N5\n" +
            "44oy9MS90/IVeRsfL0fT0pPGao2by4eja5di2j8UiiyuWSUI4J5PqBn48pNNYt4P\n" +
            "W0MKS74ea/lDWmsGWw9B/mGZ/0znIo2WUCpifIGBqUfqfmBAFjMU/4aK1bMfzhZc\n" +
            "OJNcpa2iOgrqd9Irjrs0kM5j0omN42be65FXUEoTVH5zOJr9sHaThTVM8PZ+A5Vm\n" +
            "uSxSBG00WgrVuuCixz1DDrXI+LR/ipVU/fTGWRywBQRunrQsY7V9ODtmSipczi+o\n" +
            "udwFdKx9QrsPEA==\n" +
        "-----END CERTIFICATE-----\n";

    // A client certificate signed by the "Testing CA"
    static final String clientCert =
        "-----BEGIN CERTIFICATE-----\n" +
            "MIIDxDCCAqwCAQIwDQYJKoZIhvcNAQEFBQAwgaQxCzAJBgNVBAYTAlVTMRMwEQYD\n" +
            "VQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1TYW4gRnJhbmNpc2NvMRYwFAYDVQQK\n" +
            "DA1DQVRlc3RpbmcgQ28uMRAwDgYDVQQLDAdUZXN0aW5nMRYwFAYDVQQDDA1jYXRl\n" +
            "c3RpbmcuY29tMSYwJAYJKoZIhvcNAQkBFhdjYXRlc3RpbmdAY2F0ZXN0aW5nLmNv\n" +
            "bTAeFw0yMTA1MzExMzU3MzhaFw00ODEwMTUxMzU3MzhaMIGqMQswCQYDVQQGEwJV\n" +
            "UzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzEU\n" +
            "MBIGA1UECgwLRXhhbXBsZSBDby4xFzAVBgNVBAsMDlRlc3RpbmcgQ2xpZW50MRQw\n" +
            "EgYDVQQDDAtleGFtcGxlLmNvbTEpMCcGCSqGSIb3DQEJARYadGVzdGluZy1jbGll\n" +
            "bnRAZXhhbXBsZS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCp\n" +
            "izibTCEp21PffO6UN8wX7PW+dd7HTepbwqd2mT3iYqC2/IuO8mzS5Sk/mNqL2T9T\n" +
            "GPcx4cLRGGovBw/Ig3A3nv4uBxzFVHJqtHZJl+VPgZiUDmyBaNsv0g5fqluiktcU\n" +
            "MI0HQWABOEcwCyBYHXadxp8DUAUdjniopax7cMTujrWRFJAH7s4++yu5kK06mMwD\n" +
            "Rsh65CGKvP5tz+cszoabo5I8dT+8tTPHrgdaCqjI9536VtbgmU3LKNG1DNkmR6nq\n" +
            "ICo1Lg0+GFnkFnEQpbV8WsckwFempDND3MtZKw6U5VxGs+EdTj0s0UEbCNVwLkW1\n" +
            "uBFyfcnODpWe2VY0wPafAgMBAAEwDQYJKoZIhvcNAQEFBQADggEBAHf1P4+fzuln\n" +
            "KHIBpkZTE++0tW0KUwoXfRIWATOp3XFqYJTHQm4KtTQ+mVo5FejavC57KrgeTp73\n" +
            "mljxKJhzT6Oh89shLxYPF/mvgfhWgDpwPLXmFkepigZ88MLeMV3UG9MJVcB5UrTv\n" +
            "RTxire5Ga1iaWRtHHeY4OXKp1foBxPyc9l+XHfxHeAjM4Oj4UoOKdV9sr+UYKXP7\n" +
            "N0QBXdTHa70mhzOQP6PE5VUu98HFAA6oYlbIWwetXCBDdJ1sTeh+Uw5NAzfsUw5S\n" +
            "NGib2Ru0SeKvjrpIzOto5DO6ZQdlb3Yfbj2S4ea3HBUHe6dNYQNa+dnq4xvrA3Iv\n" +
            "CT7KNBGK8AQ=\n" +
        "-----END CERTIFICATE-----\n";

    // A server certificate signed by the "Testing CA"
    static final String serverCert =
        "-----BEGIN CERTIFICATE-----\n" +
            "MIIDxDCCAqwCAQEwDQYJKoZIhvcNAQEFBQAwgaQxCzAJBgNVBAYTAlVTMRMwEQYD\n" +
            "VQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1TYW4gRnJhbmNpc2NvMRYwFAYDVQQK\n" +
            "DA1DQVRlc3RpbmcgQ28uMRAwDgYDVQQLDAdUZXN0aW5nMRYwFAYDVQQDDA1jYXRl\n" +
            "c3RpbmcuY29tMSYwJAYJKoZIhvcNAQkBFhdjYXRlc3RpbmdAY2F0ZXN0aW5nLmNv\n" +
            "bTAeFw0yMTA1MzExMzU3MzhaFw00ODEwMTUxMzU3MzhaMIGqMQswCQYDVQQGEwJV\n" +
            "UzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzEU\n" +
            "MBIGA1UECgwLRXhhbXBsZSBDby4xFzAVBgNVBAsMDlRlc3RpbmcgU2VydmVyMRQw\n" +
            "EgYDVQQDDAtleGFtcGxlLmNvbTEpMCcGCSqGSIb3DQEJARYadGVzdGluZy1zZXJ2\n" +
            "ZXJAZXhhbXBsZS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDA\n" +
            "7nYmXdUcCgkDnwDPV0MEjbNQvVNxL9Sd/zJZBy02jcn2mklBJtAb18Nvra7tw14L\n" +
            "KmEILFC4GwKVEtF1vqE6gBCxaohE0ZZ/dJGgPVvuRn+r1nzIwT4LKmDoayfne29K\n" +
            "w2oDvxX+0vU9BI9c2N8ZN3Ge9n/r8Rp25SP0cyRKa8Q862kPXgsFsYX/D/mZpK/p\n" +
            "KfwhLGl/6tCKTsOteetEvHPnirHIYgb81zI5NGirrckO6PJ3b+PJzdjgW3/12jWg\n" +
            "9JrqT9Wg3Uf3VESGiUoAr9xnMun026jpWhRJL0zzZnpP0hr8RYAJ2Mfh8JpZjlx6\n" +
            "0ePzHPTckRydLPc8tvCvAgMBAAEwDQYJKoZIhvcNAQEFBQADggEBAHq0euTu0433\n" +
            "nPwt9CuyC10o4wyTBCW0tXupM/06Hqk0U9rOQsPw0zNuQaW/Ww1rqpyG8S3Mw27f\n" +
            "oua6Usfnwog5eREUi1XGe2HcGeqSca+34+WPUyi6agS2NEqpXyZWiRgfLUEngo3d\n" +
            "Ph1BFHsxOXqLk+LCouA1OxeS8WZHdMRt6lzP+3FEJhiiEBAF8YIoQD6kD1lVBo4J\n" +
            "e5Vof1Zs6frxbi12nh/Iu7YUEgm0IZ6X5GSs5c+nj0hHdPPN86Pul3fc9tWJ8MJO\n" +
            "KYmsEX4YiIebo+dzrFEZywSQXOG5xkMhOprdc28stiIGkJCRSf/1lPy70i5YHMjr\n" +
            "VqwFqu16zZg=\n" +
        "-----END CERTIFICATE-----\n";

    // An arbitrary self-signed certificate
    static final String arbitraryCert =
        "-----BEGIN CERTIFICATE-----\n" +
            "MIID3jCCAsYCCQD5JkW9FPJFYzANBgkqhkiG9w0BAQUFADCBsDELMAkGA1UEBhMC\n" +
            "VVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28x\n" +
            "FDASBgNVBAoMC0V4YW1wbGUgQ28uMRowGAYDVQQLDBFUZXN0aW5nIEFyYml0cmFy\n" +
            "eTEUMBIGA1UEAwwLZXhhbXBsZS5jb20xLDAqBgkqhkiG9w0BCQEWHXRlc3Rpbmct\n" +
            "YXJiaXRyYXJ5QGV4YW1wbGUuY29tMB4XDTIxMDYwMjA4MDkwNloXDTQ4MTAxNzA4\n" +
            "MDkwNlowgbAxCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYD\n" +
            "VQQHDA1TYW4gRnJhbmNpc2NvMRQwEgYDVQQKDAtFeGFtcGxlIENvLjEaMBgGA1UE\n" +
            "CwwRVGVzdGluZyBBcmJpdHJhcnkxFDASBgNVBAMMC2V4YW1wbGUuY29tMSwwKgYJ\n" +
            "KoZIhvcNAQkBFh10ZXN0aW5nLWFyYml0cmFyeUBleGFtcGxlLmNvbTCCASIwDQYJ\n" +
            "KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMrvljMgkh0nLoNH9fNolHJWZ74xRDYT\n" +
            "fjpMjNvJZqKvveQMhIWgLFkPM1fMsXQEeXxfrLMEt5bh0Crbr7GfODaoltH+HM8d\n" +
            "xoXDqaXcDFtRdVyf4nM7RdwEQGhZHfa3oiewQG6UOrWpOU5s4GQL0Pk0oKNZFRGJ\n" +
            "nR8hyPxnyB5p+M5VBOO/ATuOsKS+uXYjjI5ndof8bTx9xDykebiMTLmfy745req6\n" +
            "KnaLC7NKSHsBUEvJlaQrRzyK5zouEz0Sfk3QfvnCOoLOng11WZ3I88jTl69J2X6E\n" +
            "TLwAiNrsI24zvuCa24jKeehBvDIGjfa68+YQq7Zkdlsere9EoEe7JSMCAwEAATAN\n" +
            "BgkqhkiG9w0BAQUFAAOCAQEAEwDID0JaJaI2WQMT0oZ9+Zz8Mf7wZbngrf9uG9MW\n" +
            "oljQNwtFtcUYcMd6q+wsGd24bR//YBGkAXke6x+DNyRtbbTbFLFQgko/7Do6jB7U\n" +
            "p75oNJgRBKsNO6eGIaLBxIYWxZcZ77IhRoX82WPRRLaT2iAc8p2I3QMra5Y+E+Aj\n" +
            "f9gKqRKzwDgWOApD27KosJJZd2zddHZm/Fj+T8kWPTHFCt0FnzgGAVxKp1bFmzxX\n" +
            "qGGFKW4IThE87fTFcgbRkUZdnrKTo6tCDDjIi2Rb2jJv7ip6BvI2cHip+UvQmvhe\n" +
            "qRQdDY0scDekIRZ0WQYg4h/kmfNp9Zw55Ce18aMTXEwGrQ==\n" +
        "-----END CERTIFICATE-----\n";

    static final String clientKey =
        "-----BEGIN PRIVATE KEY-----\n" +
            "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCpizibTCEp21Pf\n" +
            "fO6UN8wX7PW+dd7HTepbwqd2mT3iYqC2/IuO8mzS5Sk/mNqL2T9TGPcx4cLRGGov\n" +
            "Bw/Ig3A3nv4uBxzFVHJqtHZJl+VPgZiUDmyBaNsv0g5fqluiktcUMI0HQWABOEcw\n" +
            "CyBYHXadxp8DUAUdjniopax7cMTujrWRFJAH7s4++yu5kK06mMwDRsh65CGKvP5t\n" +
            "z+cszoabo5I8dT+8tTPHrgdaCqjI9536VtbgmU3LKNG1DNkmR6nqICo1Lg0+GFnk\n" +
            "FnEQpbV8WsckwFempDND3MtZKw6U5VxGs+EdTj0s0UEbCNVwLkW1uBFyfcnODpWe\n" +
            "2VY0wPafAgMBAAECggEAZvBnsyq66/4F46in9ogWO+ScpEJOu/XbuFDsen66ayx0\n" +
            "5gVZ+rXISxfmPn3hG44Q+7QpyjiHn4rSVbFU7OqZBLxdGbcpycnnGlBtjWtTSD2o\n" +
            "VSSYzs3KXzOLlJwLvR6oxdJgniocT0FLP6lRvw5MiakhvNIl+Pca3VKR8fTbLPet\n" +
            "wXsTSQ+80GRAUyLMKMn1Xs7A6AIrXodFmwuVFB8MUdAf8g6jGEacagz26SBzl34v\n" +
            "YfcF6RWKnBJOwz0ej0sCX8rUyB5rBqGO/z9Ey9XzuaCYJgc4SBwcohsvVsBCGD+E\n" +
            "8aXYPC5v9nWOSBRnzZruw/hg/j8YHP8Coy8bavtTEQKBgQDWVM6AI7CDH2wbqKdL\n" +
            "L3S4cILpQ4nP0ajF7Ql5TCAxLTWSCvryMSBeas4tnG5oM9+z5JQiIzveCOqtMd8s\n" +
            "geysLwk39wBN50uZPiOSlq+PxraPibDo9cI/hmi5IKIXqxnJS3OQ1LqJ4vDun7Yh\n" +
            "rsXhk0UN62irRqXTzVY0qPlZBwKBgQDKgVvi63Lm5KVTLDB8Q162Hr83QV6VEMjK\n" +
            "yd3AEaps+Q1McyKuub6VSucj3JyTZXDpUYUSoZaRrD8qqNDZ+h/uxHs8d8PD+jW7\n" +
            "c3z3YVkeUezAUOdwZMvQ0SXa1zdf159rRAKl7Xj+VLhR0+3HXxw6RYsxCPhfHYy5\n" +
            "Lukh1BoHqQKBgBFrBvUm8VtWnGSLCj1z99pdWmY2lOaMtViQcOqoox0b/XSG6+nu\n" +
            "0CCcMXFHezmArbdi5h74Gg9rThcRLH/jdyZvFCK2MhIir+QeRqnNEStwDLoRiI0G\n" +
            "G+kptS0GV+Xwg8H2HcgxYY9/H/FkjVqjZ3VzkHMXJIR201cpIs5YxRrVAoGAM+CB\n" +
            "vpccn2PRqoX2gc7sc3FbAPfBGCTtm22tXifoZfRDYONZ7jLtTOecYQaCIgxpqYvV\n" +
            "sFku7nCW2gHXRxAZoBw7idkQkKMHotbKG8GXh/nq0bWoJJXd1MfPj8l0iRv+3gbV\n" +
            "OtakGVtwwJ2vG1UVMSRhrRUkM5GpXENVO/JPHMkCgYBn8rw3sPZ6gNk2ttF1yKGA\n" +
            "Kr3Wb2rwcG5Pf5ESCNktuvY6ipxnOGuvmbMnWMQe9KnEbFrjiEDgYTxcnHztPAt1\n" +
            "cK8KDB8cBrj2OHIMc83Yfon5mM8VybTTWx3Cd/AAfhyNB0vohbJNeHO80sQH+2Cn\n" +
            "6aAcVdLuUdYcYq6RrIPM8w==\n" +
        "-----END PRIVATE KEY-----\n";

    static final String serverKey =
        "-----BEGIN PRIVATE KEY-----\n" +
            "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDA7nYmXdUcCgkD\n" +
            "nwDPV0MEjbNQvVNxL9Sd/zJZBy02jcn2mklBJtAb18Nvra7tw14LKmEILFC4GwKV\n" +
            "EtF1vqE6gBCxaohE0ZZ/dJGgPVvuRn+r1nzIwT4LKmDoayfne29Kw2oDvxX+0vU9\n" +
            "BI9c2N8ZN3Ge9n/r8Rp25SP0cyRKa8Q862kPXgsFsYX/D/mZpK/pKfwhLGl/6tCK\n" +
            "TsOteetEvHPnirHIYgb81zI5NGirrckO6PJ3b+PJzdjgW3/12jWg9JrqT9Wg3Uf3\n" +
            "VESGiUoAr9xnMun026jpWhRJL0zzZnpP0hr8RYAJ2Mfh8JpZjlx60ePzHPTckRyd\n" +
            "LPc8tvCvAgMBAAECggEAScjpFrM8FYUg/WmJ/cH5t3wZ3/8IMnmAbwxyTOoZuItx\n" +
            "egZ3jZsya/OQot1h0Tyucsa6ZU3NcRujWS/hO460SpM/zxpXEzq0u/nw17+fsPj1\n" +
            "Stq0znJZMBv9A+Y3VKg4X/dsTBKAbvxvHe7ohTHL4PD7WzgapDmJTX9EyPBgKLVz\n" +
            "+6z6wOtP2KwjtgWBipm3gMFNUSJVEtgD4XQP1nOTF+lUI51dhW9iVFbx5l85zrtX\n" +
            "gzqTMNVxQTPpdTvILUmjJBFEgZcGrq8mGYcXSHZhdIgPYLk3G1bexvtUTVzq8zbV\n" +
            "/vrCADb25B/1jT+u+txAkh9XpJM/sQFylUoo4EDGgQKBgQDss0JVZ4504zqlncFe\n" +
            "IizOOEy1Ba+M5DNZCqx9RlHJdMTDCdJi0phHvvaOhwXvyKLJkJuzxByUb5QthQ5p\n" +
            "Yc1yU14ps8K7d9Ck8HX3K035SPprYQrBd0XMHzaDS2+2zr+/c+xyf/xMTC8iaUvl\n" +
            "lizbu8O442PlyMHUbrVONeS/xwKBgQDQqZl8biSO0ljYBgYNc8INtS/TTFq5lUqP\n" +
            "LgFfi1He2wj44nGd8iSHuNbaJJCv0UJVjKlZYpazN4bwzJSckrlU72lp6WFNapqF\n" +
            "CDRV295PDbwaE2NRi2pJ+inTFpN1nvRC2Rx54UVKBDDcU+bZQw/WuQJ3LQcMlSVz\n" +
            "QYoQXb6X2QKBgBSkES3HaQnSYuPcXOdrjYKyMCY9B7D+mWezYZVPE4TA1QO5EIqj\n" +
            "mLnw8ik9pwvg8CkpnhpQCLn8/Ov3RWl1KOhGUtjKHzof2ab4fSD/ur35WjUQ8lIq\n" +
            "p4CEXEmYw3Yqk1gLsNvPQ14X6qhSjFbKAMFsn0W5NpXsKtLukIrwcjEzAoGAWGql\n" +
            "KP6a6xHip5bV1blpTtmprEU8ZEsITudVmaC1TlNN1/hL4HuMUx5VnBXGYVmwXAPA\n" +
            "dqm55bLvsPVfO4FImt7fsgs8OcukMh6p3n/OEX1maT4x5YnHvhUMx+9XCI4UPoc0\n" +
            "88gqzhQ8h//dX8501a2Lh+hChmhkeBQbZpfyfPECgYAj5dPvdITryOyjsEzkPcex\n" +
            "zhwrgrdrSk/B+AlhmT3y6AwQ4ZI0uABJeVMpzQE6sS7lMkg7intgGIsienW60vdH\n" +
            "oGxJxL7KGnzcY32N66a9SRReU3k6KWC0dMbWOZi6ebLLDXcjc4DIlUscUcHFEwfS\n" +
            "AkZ3d5ywA+rEzXzaAquoBg==\n" +
        "-----END PRIVATE KEY-----\n";

    static final String arbitraryKey =
        "-----BEGIN PRIVATE KEY-----\n" +
            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDK75YzIJIdJy6D\n" +
            "R/XzaJRyVme+MUQ2E346TIzbyWair73kDISFoCxZDzNXzLF0BHl8X6yzBLeW4dAq\n" +
            "26+xnzg2qJbR/hzPHcaFw6ml3AxbUXVcn+JzO0XcBEBoWR32t6InsEBulDq1qTlO\n" +
            "bOBkC9D5NKCjWRURiZ0fIcj8Z8geafjOVQTjvwE7jrCkvrl2I4yOZ3aH/G08fcQ8\n" +
            "pHm4jEy5n8u+Oa3quip2iwuzSkh7AVBLyZWkK0c8iuc6LhM9En5N0H75wjqCzp4N\n" +
            "dVmdyPPI05evSdl+hEy8AIja7CNuM77gmtuIynnoQbwyBo32uvPmEKu2ZHZbHq3v\n" +
            "RKBHuyUjAgMBAAECggEBAMi9jtosUdy8sWnzePu6a31es2iT22GSjr6kkoGnC/vJ\n" +
            "1BENwNldxACk5KjpNnAJLRM2oOLEu8ZowT5j6bvOQBDxW5+FuoG2dnZDQkFrFl4O\n" +
            "igWBssNB0qz9F6kg3l7671BLLLE1t42TQ7isQps0hRa+VFjA+fJLKj1tch8bmf1a\n" +
            "Gfcscs0ornwerXlMtRXcunVh64/aRQpT7/f3RhDvucASXrly28gLh1qx7AxtpgJ0\n" +
            "tHtaenq4yYJujZHTJfIkB6WzAwT0RhVJYL8hm+DjJePG65u9b/W2oHCt3Mi3Rj+b\n" +
            "XlDEPFmD2SXvuu2tenkEHkOMQRQpoOh69Vc6d/bQlMECgYEA9yNHnHrctc6N56k+\n" +
            "zCPMI4nvgWEeKPczdfVmzYQonwqNhPim5R70hAKmRMznrcvPjCNH71u63BvPfMFQ\n" +
            "PasKCBG10TGIRlpT2V0uOplTenV7xeOM7jVkQpjaz+NN7PoApL7bv9vCLL0l5TGS\n" +
            "t7+s8DyIjRHJqxHmgLhWTbk4aMkCgYEA0jaIv4xEq9rjhneQ3cjNNn6hbunXqRuR\n" +
            "8hxwDYFGhl6LcxhWF7chQGNl4TTJvXtTms4Umt+vcTJL7iSkLlpITQBWuyU310cL\n" +
            "1+DXEh5J4f3iqmgkbdsVHcYmOOLQnuYssn7+cblB6dadSB/mZui6zkIrvopnoLVb\n" +
            "YL9DjxioQIsCgYA5NZaHN73N7GHXJcuesA66j1y9I4E61Ha6MLO6kYRhxKycAn+H\n" +
            "/JF32bEprhFXnx2NgEFPvHlWKK3wYEO18tkgoxDmu0OjnZdZcwOXlxTG/VlIpvNh\n" +
            "1UQ/UmkcxK6uU/VALdpq4HFjr+mM09v1404iUrD9jweTLVKhq4p29ZCEWQKBgQCO\n" +
            "mD+a7+OFUC4XAPRb/eJ2nN+VBTstk24k9fVss8zLSUb/A/siiy8bJlHtuok+53GH\n" +
            "CVQg2qt/9cZb/K8CYmu5EAnFWTHP7nmyLuq1d6ZWjoo7XfmYK4zfbZJv9CvgHfMk\n" +
            "AdFIA4savGJkkn8QP764O1rBHdG9ykf6EMQbRXackQKBgH4GygOQ3ndk+h+ArM78\n" +
            "P8v0Cn8/30qHyQH8X8y9/BEk3Wz3aJ+AwgORaW4FZxVelWiOTJgHdZNy6Rg90q7I\n" +
            "pKHE9hkL/dxsEBXkVdaRHzrfxCBxsP3b3EmOJ4xdl1phGhedUk/+RUWGY8VSnyaq\n" +
            "uVMy1feUqV+AebfUSPzPVubV\n" +
        "-----END PRIVATE KEY-----\n";

    static final String encryptedClientKey =
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
            "MIIE6TAbBgkqhkiG9w0BBQMwDgQIZuy9oMDzt8wCAggABIIEyH7Rbg5VcLV/ZDqp\n" +
            "AJ6tzPqMlnLVtspB/qk5nZRz2/n42yaqgO2eO7DJ5qNbBGZ6KzLosC6DHd1aXA7E\n" +
            "8l5RTWQj0u16ANeKsjJ86o5/MRJkeUMh2tPLdVcG0e5nifgbWzXYpS6LuwKQpGEL\n" +
            "ufSAWhF9BY1hMe58qzCFBsFjrd0tB232sDOnhopaLiO5tQ6h3iMvwK/EPXJGPKHX\n" +
            "5++DlV/QNaENb/EsCvPlrRgpSWn6dAJ1kemKcvLbnHzTwK88B1n3GUr1B3Z+0jSV\n" +
            "9IGNeIRVz97KGxW0v33SD1Kw4QpPH+PHH/HhmJ48Vv/mf1rINwA/yRE0iS89glhW\n" +
            "NPh5O7HD2l2qZZDq74RUfvSzaW738/108ppck/uo+nOvEHnl346XJ7Etbp+fRYj1\n" +
            "y/irfT7yZ1ZMUkms39M+yBwCp/AqdhvJemOLCAgSJcWWWsNnNqlQlJJsuvkcRPBr\n" +
            "foYAsPWnZRcowZLyQ80gG5ZKoTa8Kyf/V2CuULHwYv16MLf0koLpGAEjd4QDLrun\n" +
            "ybsRisrAFupgxNBLovyh6I4b97n0Qeotf/iIfmBOH7azYqpmvgJxNB6hNVwz2LAM\n" +
            "JgnirRgY+FEdAyTd44/qPQpwXoz4ac3wrdGEU0oLwKk/J2bX1Eqh3tfziPycCO6v\n" +
            "2IC3WGP59/ty3T1xA/LI0TqInAkUD+0Rkp1HhmPL4xEL/bGLL5FViFkybjO6pgoK\n" +
            "xjq/dqTbrnmjyhVIjfdCbl2MFEsqUgemH/5J5o75P6mS0HOBaK6ERI9tM6dHz5Wk\n" +
            "2AbwUUvEebLA6zR5+57dFKSQXhOOekRCQ9w8gDhPHMxyYp3YAdiFitweY/5Xhokt\n" +
            "l3OXtLPNiqONcYp/Xkz5/CgsjoIHDGgQGy4sWYUQYCdAFXP8/ogR4zVpsUBP0Hjv\n" +
            "9isg5MlXEEj+YOFYQr/HbON9zxP4LhzuYUWynvVJDfPxY1x7JI+aequHZRPj9Qct\n" +
            "X6Va5c+G8q87fj/qCJCplGslhJsxBidTQw88+wNeVAVPd+456bszwIE5ey7glvTb\n" +
            "gwmU4mXDm75T+PW3Y7Gz1HZ9ZCMuUBelBEKxVSOOXCxkO4zzfI9PVxVzkqfurVOO\n" +
            "9R9i3gV3J6rQj98XtbvBTSrq9nylnzRiFSP2T0aRNLVRmGBahm9YIK+kll/VM2uL\n" +
            "VzUYPJv22Q/cXR2lkwNFbScOGNzJ3UXiuWOM1CkBgadoHpKhmD4IypvMpA/B0tTL\n" +
            "l4kL0z7Cg50kXYY77pl6cxorRnPs9fTpPWTPymqWp8Se+pfn/Fxqz11xD0P45Rlo\n" +
            "0CHLS3536c2zX3/1Uh4ZIGTAwoWeiCfizTnaS7GbIniqJ1/KVx9L5gIu/uWHGSnY\n" +
            "sU/gOHQw+HxkYGXYI5AqWUp79CZUEnKsNVZDAexKWraG3TBJSydZcyl3OcEfez+f\n" +
            "ISw//WpK5Wc+Zc+KabT8eWGwN1bIV/5HP1hSx6kggtQBs6BSkEDirzzzvQeIvdol\n" +
            "ddjGSudEjO2EBE25utg+5omiR8uCQwwInNNOr2otMikmWRgQ3AuIuqxYAF0qH1+f\n" +
            "n6ePw/b30oy4xIKWz8Kxrn8oPLYgs1Dl3lF6LjRwq1urC2leYb6ZPaX5QjdarMJp\n" +
            "sxOT5nOmsaG0vinlsQ==\n" +
        "-----END ENCRYPTED PRIVATE KEY-----";

    static final String encryptedServerKey =
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
            "MIIE6TAbBgkqhkiG9w0BBQMwDgQIto3hFjULIx0CAggABIIEyNWG0f8x6yY3XeE+\n" +
            "WZjuFYe5WG4ftC8hvKgYCY1NhkFSH3R4FIkAdRanuskUI9rU6tzXEEVKpnsMNTf5\n" +
            "AhQz/bI4fdu57vyz/ujwmdBMtP9m5hBjyfcJq6w/g1bECC6garbeTYvXgUljyV1n\n" +
            "h8WQ4rClFhAZ1Qrujc6DTd63BGVoGxY+Jxe6FA2mZj2Z1bCsiWFGU0ShnU+QDF/O\n" +
            "2SPcHHy37uckIdSrSFAuY8+iwQ5fL5mRH3Gw1CopQY07R8RKyRKpuZV56zJDXmGv\n" +
            "j0NfJul26EmqXJSTLJyMlYdIP+xSPo50qTWqOl7w37fRcAw7duVP2jIlHJEJD9Y1\n" +
            "UJe4ypihLp402Amw19wnsaxwk9PDDUd7kN1xFQMJ/eVF5k2hEL3m+2g6PbuFoSHt\n" +
            "FVQEMDGqeAIPvrUr9FwO0qU7x0hJ1ce/v6IyYhJwbuVDzUFP0nHalTSVM/lXOshO\n" +
            "dUqY97hOMh+q9drNUCQM08gq3XD/HQP/zA6LYH+X9Ts9Y0R+cocOXwbxc2jKAimo\n" +
            "MwmP9bSD9MPabGGTv4ZeTVK9JTq4uetCk3ehqNEpS7d69bq0pJ3F6xvGaL7GjiQA\n" +
            "8YyEUFtbyaBDgg4iwIiPsy/jo7h6Gj1Io/TjcDnp38h1YMTrER2nljB8lSm3t7DA\n" +
            "5KOzLKHvvB8gUfMiM3OzbIcXtBJIWWidELToqEljAVHWt054yaI4oX1laHOa3zPG\n" +
            "yriTUARkp5lA2llrm9E1AevlOxpUz3cr94ohvSI40QZgehcvraHJ7FCP7SUi54uY\n" +
            "o7MRDe27zLsjYRoSLD6saSFL1XigjCa4dpOu3yN8q/R0NyjmBySxOlPbglyl/OmD\n" +
            "czWFvUQcHcuSsPosbp30nsDD5SIGY6p+XR8tqJdKrCh+vgFlQPb1OkvGcXD7uzjI\n" +
            "0jYVc23pUwrM3oynAjll2uEBlMfHsdYTB5ehv/sp7tqnc3/VbPhW9WOercbQvVnq\n" +
            "Gb13aL4ra94glERJFH5LitvmjkylD6tjOttDONYg9ow1dA5g2q/A7WD0UV4TrW2w\n" +
            "LRze30kKG2p94DE1if54VMA91QLhU3YwntDSk8jZ6bO7rG91d98npwHWZfMzJI6J\n" +
            "blHM5gaAmHCTvwX6/Px/tS+8HmG5l8BBBqz70gh24uYjMgsTWsfRsjfypBmXzEFt\n" +
            "JiFJIBwGQ0uHDM2JXqIdCf1em875eK3XR6qvrQDPiA5HVXFaGdRGbx0aK7lzDHrn\n" +
            "7cn02kNLb4/kbZnpj04p7/j6dpfGLMnkSH7Pxx4f0cfirfDvcZQwjpl/n4bHnIxT\n" +
            "Qv6XWq67dQObvbiCZF14CayEEqNU6Q9wq+EBji2739FhAAbZ0P1Uj792wk50tOgg\n" +
            "+EqXzCGaqSRn7RrMEiQQTGBson9L6aBlNxyBRH6LMLAsvcKzlYB5zMQHwObd/eaU\n" +
            "43WdwOly9CLeJbTpPNvZoeDGT7jdin3XiuauMnDa4W1wwKHKgpbD543ScZfIkNm1\n" +
            "Wcibp27rE4/5eIsNyQnxkDRN7f3x0a9iwhlgrToXGabileLsonx3Jdf/YXgiqiQY\n" +
            "4lLGr4EIVjKg8YbJvpnrEZQNeTHwPr8/N5N51VZvoVePBq/ZnBi9EK/aCL+f+0Dx\n" +
            "yZio9eDGsuOXwR7iOg==\n" +
        "-----END ENCRYPTED PRIVATE KEY-----";

    static final String encryptedArbitraryKey =
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
            "MIIE6TAbBgkqhkiG9w0BBQMwDgQIT+Z6cmw0caMCAggABIIEyCCZWzPzrAGr7nsi\n" +
            "ifNxQbX0GO6wYb3ND7yinc/0Yw+d38l0S5LTLg+5GD7SbXfoMmC44rUxSGnBV5Bj\n" +
            "/bUW30B/s0+I3Ya1pXXyVP98vjy71Pspw/sHH3tYU73fPdqzh538Jf9qEgqgaJej\n" +
            "MRdiQfQmXexmkyeWAYQutbONrwfuH8NqLGxuPjrc21WjKkijnv9X8CuUk6M1MCPc\n" +
            "jN2iPhTsYiOkgKkyjF817792jDBYJHtoOuG5OmhSAdw6I3Tm5TRqAO/6IGPuDqWb\n" +
            "7ho3ZyLBxBaLHTOawQkGm842lhwn5fACCCYNGsQhVGdbZN4KVgSYzJwvGw8vaawE\n" +
            "VhD+zcDzhAydlhMXwFxDMgoZD5qRZujuDoBLgBYrDFDDEUfDNoUJSmWPpHXVc2tq\n" +
            "RBiP3xb152lWiqRcXLZVHfXfxT79qZAxvlRBUgYEA/u2ud3p2/7R9UBvRzE6P6IA\n" +
            "9ZuO6CgJgGvHXY2u721/MvvhY/75X1c1t0VJsdU2H6+E34QDtSqmQD08zBlZw5TK\n" +
            "qltQpxgjMO1jtdEuHWPbR5Aoa5smOL9qBleptSGsm0I5zET+hv4y3f5AHIx3X3xy\n" +
            "UH6Nw30oQtSbbZzPX4sqZuzA66W9Z4yW3Pn1EYcpuacypty/gvRZrvnwb6gYNB+h\n" +
            "XTTaikNiGebcAZhSukcKmL3mjlpbUZC3Bjbz5AstZJyd0tMSaw0E4C8VQQHYyX5X\n" +
            "lteW1O2TvHyGdeb0LNwsXzIMqsqmgsuNsms12Xx7KurxhPGk3Za2SdXQUASSryWX\n" +
            "XPlKxlQDDM9DvSQsRw6BCUoUKZ/2YiXnnL/zrY1+xegXumwolgT8Dsu6CFxvzT7E\n" +
            "16/4FeaVDB0/A5nmR71gkwm/b/JJL/Qj0H+E0rhCyZJeW9ddg4IWolEFTvCrVmHG\n" +
            "gZ8DQIHJHSPilG8gdjzJ8cs89pctswnjuLMI2wT2/jqtM76Ibr9d0eIB6CzT17dh\n" +
            "ASchmnUUXgjY/djAO8M3LuMUgu6OTCpIwAjfz7DOaakI42Dvrz0sTCkBwjD3XY7v\n" +
            "1EyxoxCPoxTlHE1BLNgT5OD0B9SPC9aQLt+DOcDXVh8vQvIGtqMbxthIoi61XTeo\n" +
            "sm35wkdiC0wSCgqPKIiz/LcyaWcOwbK2F7Q1dKluBT29L6X35FLqbp8RNZLJSX18\n" +
            "53gJJLH4Hd7XcAn2Sat+vyuY3Z3vNiFMmx+vbho3ZgGL5KTNdmpN1fM7EqpGOUfp\n" +
            "easRPOw3P1KuVqTVCvm/osf4VniV3xCvgwffJ/kOlHChnIrHGZpeBFmiV4xk7ucb\n" +
            "7PSVt8BLV2+krXSTijFepeuVckitj3Kf4o++WePE6wlPi0rPYG47IE10xHojSKgr\n" +
            "xdEonxiGL07/MEdI9+9PHLg5+ZakymRmMnEcJdpd7+WxaFy8jhvCCZ+7ppZBpYQC\n" +
            "azdkciNz9d/YZRQH5M6VGk1VqqkQbenasUuZpXAxDOraLPlq801JyYyPqeROAgb8\n" +
            "hTQXseAe+7lHWpmUGKuN0Y9EcV/3fiAP+PeMt/QXeGF5jcRVGWLs6nDVOaBQ/SB3\n" +
            "syFXwJR6WtajR5OC7dEGZPTxaNfiu4GFuE73i3cVEJH2MH7+c/yB/40RJQdJ4kd6\n" +
            "7KJYwr8P4TrNulM60Q==\n" +
        "-----END ENCRYPTED PRIVATE KEY-----\n";

    static final String secretPassword = "secret";
}
