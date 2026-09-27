package xyz.jpenilla.squaremap.common.s3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.framework.qual.DefaultQualifier;
import xyz.jpenilla.squaremap.common.Logging;
import xyz.jpenilla.squaremap.common.config.Config;
import xyz.jpenilla.squaremap.common.data.DirectoryProvider;
import xyz.jpenilla.squaremap.common.util.FileUtil;
import xyz.jpenilla.squaremap.common.util.Util;

/**
 * Mirrors rendered tile images to an S3 compatible bucket (AWS S3, RustFS, MinIO, ...).
 *
 * <p>Tiles are still written to disk first; every saved tile is queued and uploaded in the background.
 * On startup, tiles modified since the last fully successful session are uploaded as well, so tiles
 * rendered before S3 was enabled (or while the bucket was unreachable) end up in the bucket.</p>
 */
@DefaultQualifier(NonNull.class)
public final class TileUploader {
    private static final int MAX_ATTEMPTS = 3;
    private static final int CATCH_UP_MAX_QUEUED = 1000;
    private static final String CACHE_CONTROL = "public, max-age=60";

    private static volatile @Nullable TileUploader instance;

    private final S3Client client;
    private final Path tilesDir;
    private final Path markerFile;
    private final String markerId;
    private final ThreadPoolExecutor executor;
    private final Set<Path> pending = ConcurrentHashMap.newKeySet();
    private final AtomicLong uploaded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile boolean catchUpDone = false;
    private volatile boolean stopping = false;

    private TileUploader(final S3Client client, final DirectoryProvider directoryProvider) {
        this.client = client;
        this.tilesDir = directoryProvider.tilesDirectory();
        this.markerFile = directoryProvider.dataDirectory().resolve("data").resolve("s3-sync-marker.txt");
        this.markerId = Config.S3_ENDPOINT + "|" + Config.S3_BUCKET + "|";
        this.executor = Util.newFixedThreadPool(
            Config.S3_UPLOAD_THREADS,
            Util.squaremapThreadFactory("s3-upload"),
            new ThreadPoolExecutor.DiscardPolicy()
        );
    }

    public static void start(final DirectoryProvider directoryProvider) {
        if (!Config.S3_ENABLED) {
            return;
        }
        if (Config.S3_ENDPOINT.isBlank() || Config.S3_BUCKET.isBlank() || Config.S3_ACCESS_KEY.isBlank() || Config.S3_SECRET_KEY.isBlank()) {
            Logging.logger().error("[S3] settings.s3 is enabled but endpoint, bucket, access-key or secret-key is empty. Tiles will NOT be uploaded.");
            return;
        }
        final S3Client client;
        try {
            client = new S3Client(Config.S3_ENDPOINT, Config.S3_REGION, Config.S3_BUCKET, Config.S3_ACCESS_KEY, Config.S3_SECRET_KEY);
        } catch (final IllegalArgumentException ex) {
            Logging.logger().error("[S3] {}. Tiles will NOT be uploaded.", ex.getMessage());
            return;
        }
        final TileUploader uploader = new TileUploader(client, directoryProvider);
        instance = uploader;
        final Thread setup = Util.squaremapThreadFactory("s3-setup").newThread(uploader::setupAndCatchUp);
        setup.start();
    }

    public static void stop() {
        final @Nullable TileUploader uploader = instance;
        instance = null;
        if (uploader != null) {
            uploader.shutdown();
        }
    }

    /**
     * Base URL the web map should load tile images from, or empty (relative "tiles")
     * when tiles are not being uploaded.
     *
     * @return tiles base URL
     */
    public static String webTilesUrl() {
        if (instance == null) {
            return "";
        }
        if (!Config.S3_PUBLIC_URL.isBlank()) {
            return Config.S3_PUBLIC_URL;
        }
        return S3Client.trimTrailingSlashes(Config.S3_ENDPOINT) + "/" + Config.S3_BUCKET + "/tiles";
    }

    /**
     * Queues a tile image that was just written to disk for upload.
     *
     * @param file tile image file
     */
    public static void enqueue(final Path file) {
        final @Nullable TileUploader uploader = instance;
        if (uploader != null) {
            uploader.submit(file);
        }
    }

    private void setupAndCatchUp() {
        try {
            this.client.headBucket();
            Logging.logger().info("[S3] Connected to bucket '{}' at {}", Config.S3_BUCKET, Config.S3_ENDPOINT);
        } catch (final Exception ex) {
            // Skip the catch-up; the sync marker is not advanced, so it runs again on the next start.
            Logging.logger().error("[S3] Could not reach bucket '{}' at {}. Check endpoint, bucket name and keys. "
                + "Existing tiles will be uploaded on the next start, new tiles are still attempted.",
                Config.S3_BUCKET, Config.S3_ENDPOINT, ex);
            return;
        }

        if (Config.S3_SET_PUBLIC_READ_POLICY) {
            try {
                this.client.putBucketPolicy(publicReadPolicy(Config.S3_BUCKET));
                Logging.logger().info("[S3] Bucket policy set: 'tiles/*' is publicly readable.");
            } catch (final Exception ex) {
                Logging.logger().warn("[S3] Could not set the public read bucket policy, set it manually so browsers can load tiles "
                    + "(or disable settings.s3.set-public-read-policy): {}", ex.getMessage());
            }
        }

        final long since = this.readMarker();
        Logging.logger().info(since == 0L
            ? "[S3] Uploading all existing tiles in the background..."
            : "[S3] Uploading tiles changed since the last session in the background...");
        long queued = 0;
        try (final Stream<Path> files = Files.walk(this.tilesDir)) {
            for (final Path file : (Iterable<Path>) files::iterator) {
                if (this.stopping) {
                    return;
                }
                final String name = file.getFileName().toString();
                if (!name.endsWith(".png") || name.startsWith(".")) {
                    continue;
                }
                try {
                    if (since != 0L && Files.getLastModifiedTime(file).toMillis() <= since) {
                        continue;
                    }
                } catch (final NoSuchFileException ignore) {
                    continue; // deleted while scanning
                }
                // don't hold millions of paths in memory at once
                while (this.executor.getQueue().size() > CATCH_UP_MAX_QUEUED && !this.stopping) {
                    Thread.sleep(50);
                }
                this.submit(file);
                queued++;
            }
        } catch (final NoSuchFileException ignore) {
            // nothing rendered yet
        } catch (final IOException | UncheckedIOException ex) {
            Logging.logger().warn("[S3] Failed to scan tiles directory for upload", ex);
            return;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
        }
        this.catchUpDone = true;
        Logging.logger().info("[S3] Queued {} existing tiles for upload.", queued);
    }

    private void submit(final Path file) {
        if (this.stopping || !this.pending.add(file)) {
            return; // already queued, the queued upload will read the latest contents
        }
        this.executor.execute(() -> this.upload(file));
    }

    private void upload(final Path file) {
        // remove first, so a save during the upload queues another upload
        this.pending.remove(file);
        final String key = "tiles/" + FileUtil.invariantSeparatorsPathString(this.tilesDir.relativize(file));

        for (int attempt = 1; ; attempt++) {
            try {
                final byte[] data;
                try {
                    data = Files.readAllBytes(file);
                } catch (final NoSuchFileException ex) {
                    return; // deleted in the meantime (map reset)
                }
                this.client.putObject(key, data, "image/png", CACHE_CONTROL);
                this.uploaded.incrementAndGet();
                return;
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                this.failed.incrementAndGet();
                return;
            } catch (final IOException ex) {
                if (attempt >= MAX_ATTEMPTS) {
                    // only log the first few to avoid flooding the console when the bucket is down
                    if (this.failed.incrementAndGet() <= 5) {
                        Logging.logger().warn("[S3] Failed to upload {} after {} attempts: {}", key, MAX_ATTEMPTS, ex.getMessage());
                    }
                    return;
                }
                try {
                    Thread.sleep(1000L * attempt);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    this.failed.incrementAndGet();
                    return;
                }
            }
        }
    }

    private void shutdown() {
        this.stopping = true;
        this.executor.shutdown();
        boolean drained;
        try {
            // Short wait: this runs on the server thread. Anything not uploaded is retried on the
            // next start, since the sync marker is only advanced after a full drain.
            drained = this.executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            drained = false;
        }
        if (!drained) {
            this.executor.shutdownNow();
        }
        Logging.logger().info("[S3] Uploaded {} tiles this session, {} failed.", this.uploaded.get(), this.failed.get());
        // Only move the marker forward when everything reached the bucket, otherwise
        // the next startup re-uploads everything changed since the previous good session.
        if (drained && this.catchUpDone && this.failed.get() == 0) {
            this.writeMarker(System.currentTimeMillis());
        }
    }

    private long readMarker() {
        try {
            final String content = Files.readString(this.markerFile).strip();
            if (content.startsWith(this.markerId)) {
                return Long.parseLong(content.substring(this.markerId.length()));
            }
        } catch (final IOException | NumberFormatException ignore) {
        }
        return 0L; // never synced to this bucket
    }

    private void writeMarker(final long time) {
        try {
            Files.createDirectories(this.markerFile.getParent());
            FileUtil.atomicWrite(this.markerFile, tmp -> Files.writeString(tmp, this.markerId + time));
        } catch (final IOException ex) {
            Logging.logger().warn("[S3] Failed to write sync marker", ex);
        }
    }

    private static String publicReadPolicy(final String bucket) {
        return """
            {
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Principal": {"AWS": ["*"]},
                  "Action": ["s3:GetObject"],
                  "Resource": ["arn:aws:s3:::%s/tiles/*"]
                }
              ]
            }
            """.formatted(bucket);
    }
}
