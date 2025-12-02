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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.netflix.cassandra.metrics.ObjectStoreMetrics;
import com.netflix.cassandra.util.Futures;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.Either;

public class AwsAsyncS3FakeBackup implements ObjectStoreAccess, Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(AwsAsyncS3FakeBackup.class);
    private String fakeS3RootDir;
    private final Map<String, String> envVars;
    private final String region;

    private final EnumMap<Method, CopyOnWriteArrayList<Injection<?>>> injectedBehavior;

    public AwsAsyncS3FakeBackup(
        Map<String, String> envVars,
        Region region)
    {
        this.envVars = envVars;
        this.region = region.id();
        this.injectedBehavior = new EnumMap<>(Method.class);
    }

    @Override
    public AsyncPromise<Void> getObjectAsFile(String bucket, String key, Path path)
    {
        ObjectStoreMetrics objectStoreMetrics = ObjectStoreAccess.getMetrics();
        Timer.Context time = objectStoreMetrics.objectFetchLatency.time();
        Optional<Injection<Void>> expectation = getExpectation(Method.GET_OBJECT_AS_FILE);
        return Futures.toPromise(
            expectation.flatMap(Injection::error)
                       .map(CompletableFuture::<Void>failedFuture)
                       .or(() -> expectation.flatMap(Injection::result).map(CompletableFuture::completedFuture))
                       .orElseGet(() ->
                                  CompletableFuture.runAsync(() -> {
                                        File sourceFile = new File(fakeS3RootDir, bucket + "/" + key);
                                        if (!sourceFile.exists())
                                        {
                                            objectStoreMetrics.failures.mark();
                                            throw new RuntimeException("Source file does not exist");
                                        }
                                        try
                                        {
                                            Files.copy(sourceFile.toPath(), path);
                                        }
                                        catch (IOException e)
                                        {
                                            throw new RuntimeException(e);
                                        } finally
                                        {
                                            objectStoreMetrics.objectFetchBytes.update(sourceFile.length());
                                            time.stop();
                                        }
                                  })
                       ).whenComplete((result, failure) -> {
                            time.stop();
                            if (failure != null) {
                                objectStoreMetrics.failures.mark();
                            } else {
                                objectStoreMetrics.successes.mark();
                            }
                       })
        );
    }

    @Override
    public AsyncPromise<Void> getObjectRangeIntoBuffer(String bucket, String key, long from, long to, ByteBuffer buffer)
    {
        long expectedLength = to - from + 1;
        if (buffer.capacity() < expectedLength)
        {
            throw new IllegalArgumentException(
                String.format("Buffer too small: buffer size %d, expected %d bytes", buffer.capacity(), expectedLength));
        }
        
        ObjectStoreMetrics objectStoreMetrics = ObjectStoreAccess.getMetrics();
        Timer.Context time = objectStoreMetrics.rangeReadFetchLatency.time();
        Optional<Injection<Void>> expectation = getExpectation(Method.GET_OBJECT_RANGE_INTO_BUFFER);
        return Futures.toPromise(
            expectation.flatMap(Injection::error)
                       .map(CompletableFuture::<Void>failedFuture)
                       .or(() -> expectation.flatMap(Injection::result).map(CompletableFuture::completedFuture))
                       .orElseGet(() ->
                            CompletableFuture.runAsync(() -> {
                                File file = new File(fakeS3RootDir, bucket + "/" + key);
                                if (!file.exists())
                                {
                                    throw new RuntimeException("File does not exist");
                                }
                                try
                                {
                                    byte[] data = Files.readAllBytes(file.toPath());
                                    if (from >= data.length)
                                    {
                                        objectStoreMetrics.rangeReadFetchBytes.update(0);
                                        return;
                                    }
                                    int length = (int)Math.min(expectedLength, data.length - from);
                                    if (length != expectedLength)
                                    {
                                        throw new RuntimeException(
                                            String.format("Received incomplete byte range: expected %d bytes, got %d bytes", 
                                                         expectedLength, length));
                                    }
                                    buffer.put(data, (int)from, length);
                                    objectStoreMetrics.rangeReadFetchBytes.update(length);
                                }
                                catch (IOException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            })
                       ).whenComplete((result, failure) -> {
                           if (failure != null) {
                               objectStoreMetrics.failures.mark();
                           } else {
                               objectStoreMetrics.successes.mark();
                           }
                           time.stop();
                       })
        );
    }

    @Override
    public AsyncPromise<List<String>> getObjectKeys(String bucket, String prefix)
    {
        ObjectStoreMetrics objectStoreMetrics = ObjectStoreAccess.getMetrics();
        Timer.Context time = objectStoreMetrics.prefixFetchLatency.time();
        Optional<Injection<List<String>>> expectation = getExpectation(Method.GET_OBJECT_KEYS);
        return Futures.toPromise(
            expectation.flatMap(Injection::error)
                       .map(CompletableFuture::<List<String>>failedFuture)
                       .or(() -> expectation.flatMap(Injection::result).map(CompletableFuture::completedFuture))
                       .orElseGet(() ->
                            CompletableFuture.supplyAsync(() -> {
                                File bucketDir = new File(fakeS3RootDir, bucket);
                                if (!bucketDir.exists())
                                {
                                    return new ArrayList<>();
                                }
                                try
                                {
                                    return Files.walk(bucketDir.toPath())
                                                .filter(Files::isRegularFile)
                                                .map(p -> p.toString().substring(bucketDir.toString().length() + 1))
                                                .filter(key -> key.startsWith(prefix))
                                                .collect(Collectors.toList());
                                }
                                catch (IOException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            })
                       ).whenComplete((result, failure) -> {
                           if (failure != null) {
                               objectStoreMetrics.failures.mark();
                           } else {
                               objectStoreMetrics.successes.mark();
                           }
                           time.stop();
                       })
        );
    }

    @Override
    public AsyncPromise<Long> getObjectSize(String bucket, String key)
    {
        ObjectStoreMetrics objectStoreMetrics = ObjectStoreAccess.getMetrics();
        Timer.Context time = objectStoreMetrics.headObjectLatency.time();
        Optional<Injection<Long>> expectation = getExpectation(Method.GET_OBJECT_SIZE);
        return Futures.toPromise(
        expectation.flatMap(Injection::error)
                   .map(CompletableFuture::<Long>failedFuture)
                   .or(() -> expectation.flatMap(Injection::result).map(CompletableFuture::completedFuture))
                   .orElseGet(() ->
                        CompletableFuture.supplyAsync(() -> {
                            File file = new File(fakeS3RootDir, bucket + "/" + key);
                            if (!file.exists())
                            {
                                throw new RuntimeException("File does not exist");
                            }
                            try
                            {
                                return Files.size(file.toPath());
                            }
                            catch (IOException e)
                            {
                                throw new RuntimeException(e);
                            }
                        })
                   ).whenComplete((result, failure) -> {
                       if (failure != null) {
                           objectStoreMetrics.failures.mark();
                       } else {
                           objectStoreMetrics.successes.mark();
                       }
                       time.stop();
                   })
        );
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<Injection<T>> getExpectation(Method method)
    {
        List<Injection<?>> injections = injectedBehavior.get(method);
        if (injections != null && !injections.isEmpty())
        {
            return Optional.ofNullable((Injection<T>) injections.remove(0));
        }
        return Optional.empty();
    }

    public void injectBehavior(Injection<?> injection)
    {
        injectedBehavior.computeIfAbsent(injection.method(), k -> new CopyOnWriteArrayList<>()).add(injection);
    }

    public void setFakeS3RootDir(String rootDir)
    {
        this.fakeS3RootDir = rootDir;
    }

    public interface Injection<T> extends Serializable {
        Method method();
        Either<T, Throwable> returnResult();
        default Optional<Throwable> error() {
            return returnResult().right();
        }
        default Optional<T> result() {
            return returnResult().left();
        }

        // Pretty much a way to bypass the fact that Java's Optional is not serializable.
        default Optional<Optional<T>> resultAsOptional() {
            return returnResult().left().map(Optional::ofNullable);
        }

        class Failure<T> implements Injection<T> {

            private final Method method;
            private final Throwable error;

            public Failure(Method method, Throwable error)
            {
                this.method = method;
                this.error = error;
            }

            @Override
            public Method method()
            {
                return method;
            }

            @Override
            public Either<T, Throwable> returnResult()
            {
                return Either.right(error);
            }
        }

        class Value<T> implements Injection<T> {
            private final Method method;
            private final T value;

            public Value(Method method, T value)
            {
                this.method = method;
                this.value = value;
            }

            @Override
            public Method method()
            {
                return method;
            }

            @Override
            public Either<T, Throwable> returnResult()
            {
                return Either.left(value);
            }
        }
    }

    public enum Method {
        GET_OBJECT_AS_FILE,
        GET_OBJECT_RANGE_INTO_BUFFER,
        GET_OBJECT_KEYS,
        GET_OBJECT_SIZE
    }
}
