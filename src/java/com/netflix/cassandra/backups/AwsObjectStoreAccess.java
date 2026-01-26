/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.cassandra.backups;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.codahale.metrics.Timer;
import com.netflix.cassandra.metrics.ObjectStoreMetrics;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import static com.netflix.cassandra.util.Futures.toPromise;

/**
 * AWS S3 implementation of {@link ObjectStoreAccess}.
 *
 * This class provides an S3-backed implementation of the object store interface using the
 * AWS SDK's S3AsyncClient with CRT (Common Runtime) for high-performance async operations.
 *
 * Features:
 * - Uses CRT-based S3 client for optimal performance (20 Gbps target throughput)
 * - Configurable retry behavior via {@link ObjectStoreConfiguration}
 * - Comprehensive metrics tracking via {@link ObjectStoreMetrics}
 * - Efficient byte range reads with zero-copy buffer operations
 *
 * @see ObjectStoreAccess
 */
public class AwsObjectStoreAccess implements ObjectStoreAccess
{
    /** Target throughput for S3 CRT client in Gbps. Configurable via cassandra.s3.target_throughput_gbps system property. */
    private static final double TARGET_THROUGHPUT_GBPS = Double.parseDouble(System.getProperty("cassandra.s3.target_throughput_gbps", "20.0"));

    /** Maximum concurrency for S3 CRT client. Configurable via cassandra.s3.max_concurrency system property. */
    private static final int MAX_CONCURRENCY = Integer.parseInt(System.getProperty("cassandra.s3.max_concurrency", "1024"));

    private final S3AsyncClient s3AsyncClient;
    private final ObjectStoreMetrics metrics;
    private final BiConsumer<Object, Throwable> successMetricReporting;

    AwsObjectStoreAccess(S3AsyncClient s3AsyncClient, ObjectStoreMetrics metrics)
    {
        this.s3AsyncClient = s3AsyncClient;
        this.metrics = metrics;
        this.successMetricReporting = (result, error) -> {
            if (error == null)
                this.metrics.successes.mark();
            else
                S3ErrorCode.fromThrowable(error)
                           .filter(code -> code == S3ErrorCode.ACCESS_DENIED)
                           .ifPresentOrElse(code -> this.metrics.accessDenied.mark(),
                                           this.metrics.failures::mark);
        };
    }

    static AwsObjectStoreAccess build(Region region, ObjectStoreConfiguration objectStoreConfiguration)
    {
        System.setProperty("aws.requestChecksumCalculation", "WHEN_REQUIRED");
        System.setProperty("aws.responseChecksumValidation", "NEVER");

        S3AsyncClient s3Client = S3AsyncClient.crtBuilder()
                                              .credentialsProvider(DefaultCredentialsProvider.create())
                                              .region(region)
                                              .targetThroughputInGbps(TARGET_THROUGHPUT_GBPS)
                                              .checksumValidationEnabled(false)
                                              .maxConcurrency(MAX_CONCURRENCY)
                                              .retryConfiguration(builder -> builder.numRetries(objectStoreConfiguration.maxRetries))
                                              .build();

        ObjectStoreMetrics metrics = ObjectStoreAccess.getMetrics();
        return new AwsObjectStoreAccess(s3Client, metrics);
    }

    @Override
    public AsyncPromise<Void> getObjectAsFile(String bucket, String key, Path path)
    {
        Timer.Context time = metrics.objectFetchLatency.time();
        return toPromise(s3AsyncClient.getObject(builder -> builder
                                                            .bucket(bucket)
                                                            .key(key)
                                                            .build(),
                                                 AsyncResponseTransformer.toFile(path))
                                      .whenComplete((response, error) -> {
                                          if (error == null)
                                              metrics.objectFetchBytes.update(response.contentLength());
                                      })
                                      .whenComplete((result, error) -> {
                                          successMetricReporting.accept(result, error);
                                          time.stop();
                                      })
                                      .thenApply(__ -> null)
        );
    }

    @Override
    public AsyncPromise<byte[]> getObjectAsBytes(String bucket, String key)
    {
        Timer.Context time = metrics.fullReadFetchLatency.time();
        GetObjectRequest req = GetObjectRequest.builder()
                                               .bucket(bucket)
                                               .key(key)
                                               .build();

        return toPromise(
            s3AsyncClient.getObject(req, AsyncResponseTransformer.toBytes())
                        .thenApply(responseBytes -> responseBytes.asByteArray())
                        .whenComplete((response, error) -> {
                            if (error == null && response != null)
                                metrics.fullReadFetchBytes.update(response.length);
                        })
                        .whenComplete(successMetricReporting)
                        .whenComplete((__, ___) -> time.stop())
        );
    }

    @Override
    public AsyncPromise<Void> getObjectRangeIntoBuffer(String bucket, String key, long from, long to, ByteBuffer buffer)
    {
        Timer.Context time = metrics.rangeReadFetchLatency.time();
        long expectedLength = to - from + 1;
        if (buffer.capacity() < expectedLength)
        {
            throw new IllegalArgumentException(
                String.format("Buffer too small: %d < %d", buffer.capacity(), expectedLength));
        }

        GetObjectRequest req = GetObjectRequest.builder()
                                               .bucket(bucket)
                                               .key(key)
                                               .range("bytes=" + from + '-' + to)
                                               .build();


        java.util.concurrent.CompletableFuture<ResponsePublisher<GetObjectResponse>> pubFut =
            s3AsyncClient.getObject(req, AsyncResponseTransformer.toPublisher());

        return toPromise(pubFut.thenCompose(pub -> {
            java.util.concurrent.CompletableFuture<Void> done = new java.util.concurrent.CompletableFuture<>();
            pub.subscribe(new Subscriber<ByteBuffer>() {
                @Override
                public void onSubscribe(Subscription s)
                {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer chunk)
                {
                    buffer.put(chunk);
                }

                @Override
                public void onError(Throwable t)
                {
                    done.completeExceptionally(t);
                }

                @Override
                public void onComplete()
                {
                    done.complete(null);
                }
            });
            return done;
        }).whenComplete((response, error) -> {
            if (error == null)
                metrics.rangeReadFetchBytes.update(buffer.position());
        })
        .whenComplete((result, error) -> {
            successMetricReporting.accept(result, error);
            time.stop();
        }));
    }

    @Override
    public AsyncPromise<List<String>> getObjectKeys(String bucket, String prefix)
    {
        Timer.Context time = metrics.prefixFetchLatency.time();
        return toPromise(
        s3AsyncClient.listObjectsV2(request -> request.bucket(bucket)
                                                      .prefix(prefix))
                     .thenApply(response -> response.contents().stream()
                                                    .map(S3Object::key)
                                                    .collect(Collectors.toList()))
                     .whenComplete((result, error) -> {
                         successMetricReporting.accept(result, error);
                         time.stop();
                     })
        );
    }

    @Override
    public AsyncPromise<Long> getObjectSize(String bucket, String key)
    {
        Timer.Context time = metrics.headObjectLatency.time();
        return toPromise(
        s3AsyncClient.headObject(request -> request.bucket(bucket).key(key))
                     .thenApply(HeadObjectResponse::contentLength)
                     .whenComplete((result, error) -> {
                         successMetricReporting.accept(result, error);
                         time.stop();
                     })
        );
    }

    private static Optional<byte[]> getResponse(ResponseBytes<GetObjectResponse> response,
                                                Throwable error)
    {
        if (error != null)
        {
            return S3ErrorCode.fromThrowable(error)
                              .filter(code -> code == S3ErrorCode.NOT_FOUND)
                              .map(__ -> Optional.<byte[]>empty())
                              .orElseThrow(() -> new CompletionException(error));
        }
        return Optional.of(response.asByteArray());
    }

    private static Optional<byte[]> getResponseWithRangeValidation(ResponseBytes<GetObjectResponse> response,
                                                                   Throwable error,
                                                                   long expectedLength)
    {
        if (error != null)
        {
            return S3ErrorCode.fromThrowable(error)
                              .filter(code -> code == S3ErrorCode.NOT_FOUND)
                              .map(__ -> Optional.<byte[]>empty())
                              .orElseThrow(() -> new CompletionException(error));
        }

        byte[] data = response.asByteArray();
        if (data.length != expectedLength)
        {
            throw new CompletionException(new RuntimeException(
                String.format("Received incomplete byte range: expected %d bytes, got %d bytes",
                             expectedLength, data.length)));
        }
        return Optional.of(data);
    }

    private static void getResponseIntoBuffer(ResponseBytes<GetObjectResponse> response,
                                                           Throwable error,
                                                           long expectedLength,
                                                           ByteBuffer buffer)
    {
        if (error != null)
            S3ErrorCode.fromThrowable(error)
                       .ifPresent(__ -> { throw new CompletionException(error); });

        byte[] data = response.asByteArrayUnsafe();
        if (data.length != expectedLength)
        {
            throw new CompletionException(new RuntimeException(
                String.format("Received incomplete byte range: expected %d bytes, got %d bytes",
                             expectedLength, data.length)));
        }

        buffer.put(data);
    }

    private enum S3ErrorCode
    {
        NOT_FOUND,
        ACCESS_DENIED,
        GENERIC_ERROR;

        private static S3ErrorCode fromS3Exception(S3Exception s3Exception)
        {
            if (s3Exception.statusCode() == 404)
            {
                return NOT_FOUND;
            }
            else if (s3Exception.statusCode() == 403)
            {
                return ACCESS_DENIED;
            }
            else
            {
                return GENERIC_ERROR;
            }
        }

        static Optional<S3ErrorCode> fromThrowable(Throwable error)
        {
            if (error instanceof S3Exception)
            {
                return Optional.of(fromS3Exception((S3Exception) error));
            }
            else if (error.getCause() instanceof S3Exception)
            {
                return Optional.of(fromS3Exception((S3Exception) error.getCause()));
            }
            return Optional.empty();
        }
    }
}
