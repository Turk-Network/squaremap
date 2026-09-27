package xyz.jpenilla.squaremap.common.s3;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.framework.qual.DefaultQualifier;

/**
 * Minimal S3 client (path-style addressing, AWS Signature Version 4) that works with
 * AWS S3 and S3-compatible servers such as RustFS or MinIO.
 */
@DefaultQualifier(NonNull.class)
final class S3Client {
    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    private static final DateTimeFormatter DATE_STAMP = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final HttpClient http;
    private final URI endpoint;
    private final String region;
    private final String bucket;
    private final String accessKey;
    private final String secretKey;

    S3Client(final String endpoint, final String region, final String bucket, final String accessKey, final String secretKey) {
        this.endpoint = URI.create(trimTrailingSlashes(endpoint));
        if (this.endpoint.getScheme() == null || this.endpoint.getHost() == null) {
            throw new IllegalArgumentException("Invalid S3 endpoint '" + endpoint + "', expected something like http://1.2.3.4:9000");
        }
        this.region = region;
        this.bucket = bucket;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    }

    void putObject(final String key, final byte[] body, final String contentType, final String cacheControl) throws IOException, InterruptedException {
        this.send("PUT", "/" + key, "", body, Map.of(
            "Content-Type", contentType,
            "Cache-Control", cacheControl
        ));
    }

    void putBucketPolicy(final String policyJson) throws IOException, InterruptedException {
        final byte[] body = policyJson.getBytes(StandardCharsets.UTF_8);
        this.send("PUT", "", "policy", body, Map.of(
            "Content-Type", "application/json",
            "Content-MD5", Base64.getEncoder().encodeToString(digest("MD5", body))
        ));
    }

    void headBucket() throws IOException, InterruptedException {
        this.send("HEAD", "", "", new byte[0], Map.of());
    }

    private void send(
        final String method,
        final String objectPath,
        final String subResource,
        final byte[] body,
        final Map<String, String> extraHeaders
    ) throws IOException, InterruptedException {
        final String canonicalUri = encodePath(this.endpoint.getRawPath() + "/" + this.bucket + objectPath);
        final String canonicalQuery = subResource.isEmpty() ? "" : subResource + "=";
        final String payloadHash = HexFormat.of().formatHex(digest("SHA-256", body));

        final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        final String amzDate = AMZ_DATE.format(now);
        final String dateStamp = DATE_STAMP.format(now);

        final Map<String, String> signed = new TreeMap<>();
        signed.put("host", this.hostHeader());
        signed.put("x-amz-content-sha256", payloadHash);
        signed.put("x-amz-date", amzDate);

        final StringBuilder canonicalHeaders = new StringBuilder();
        signed.forEach((k, v) -> canonicalHeaders.append(k).append(':').append(v.trim()).append('\n'));
        final String signedHeaders = String.join(";", signed.keySet());

        final String canonicalRequest = method + '\n'
            + canonicalUri + '\n'
            + canonicalQuery + '\n'
            + canonicalHeaders + '\n'
            + signedHeaders + '\n'
            + payloadHash;

        final String scope = dateStamp + "/" + this.region + "/s3/aws4_request";
        final String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + '\n' + scope + '\n'
            + HexFormat.of().formatHex(digest("SHA-256", canonicalRequest.getBytes(StandardCharsets.UTF_8)));

        byte[] key = hmac(("AWS4" + this.secretKey).getBytes(StandardCharsets.UTF_8), dateStamp);
        key = hmac(key, this.region);
        key = hmac(key, "s3");
        key = hmac(key, "aws4_request");
        final String signature = HexFormat.of().formatHex(hmac(key, stringToSign));

        final String authorization = "AWS4-HMAC-SHA256 Credential=" + this.accessKey + "/" + scope
            + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature;

        final URI uri = URI.create(this.endpoint.getScheme() + "://" + this.endpoint.getRawAuthority()
            + canonicalUri + (canonicalQuery.isEmpty() ? "" : "?" + subResource));

        final HttpRequest.Builder request = HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(60))
            .method(method, body.length == 0
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body))
            .header("x-amz-content-sha256", payloadHash)
            .header("x-amz-date", amzDate)
            .header("Authorization", authorization);
        extraHeaders.forEach(request::header);

        final HttpResponse<String> response = this.http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IOException(method + " " + uri + " failed with HTTP " + response.statusCode()
                + (response.body().isBlank() ? "" : ": " + response.body().strip()));
        }
    }

    // Must match the Host header java.net.http sends
    private String hostHeader() {
        final int port = this.endpoint.getPort();
        final boolean https = this.endpoint.getScheme().equalsIgnoreCase("https");
        if (port == -1 || (https && port == 443) || (!https && port == 80)) {
            return this.endpoint.getHost();
        }
        return this.endpoint.getHost() + ":" + port;
    }

    // S3 URI encoding: encode everything except unreserved characters, keep '/'
    private static String encodePath(final String path) {
        final StringBuilder out = new StringBuilder();
        for (final byte b : path.getBytes(StandardCharsets.UTF_8)) {
            final char c = (char) (b & 0xFF);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                || c == '-' || c == '_' || c == '.' || c == '~' || c == '/') {
                out.append(c);
            } else {
                out.append('%').append(String.format("%02X", b & 0xFF));
            }
        }
        return out.toString();
    }

    static String trimTrailingSlashes(final String url) {
        int end = url.length();
        while (end > 0 && url.charAt(end - 1) == '/') {
            end--;
        }
        return url.substring(0, end);
    }

    private static byte[] digest(final String algorithm, final byte[] data) {
        try {
            return MessageDigest.getInstance(algorithm).digest(data);
        } catch (final NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static byte[] hmac(final byte[] key, final String data) {
        try {
            final Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (final GeneralSecurityException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
