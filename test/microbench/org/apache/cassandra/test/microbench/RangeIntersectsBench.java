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
package org.apache.cassandra.test.microbench;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.asymmetric.RangeDenormalizer;
import org.apache.cassandra.repair.asymmetric.RangeMap;
import org.apache.cassandra.repair.asymmetric.StreamFromOptions;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class RangeIntersectsBench
{
    // --- Range.intersects() benchmarks ---

    // Non-wrapping ranges that intersect
    private Range<Token> nonWrapA;
    private Range<Token> nonWrapB;

    // Non-wrapping ranges that don't intersect
    private Range<Token> nonWrapC;
    private Range<Token> nonWrapD;

    // Wrapping range and non-wrapping that intersect
    private Range<Token> wrapA;
    private Range<Token> nonWrapInWrap;

    // Wrapping range and non-wrapping in gap (no intersection)
    private Range<Token> nonWrapInGap;

    // Two wrapping ranges
    private Range<Token> wrapB;

    // Full ring
    private Range<Token> fullRing;

    // --- Denormalize benchmark state ---
    @Param({"10", "100", "1000"})
    private int numExistingRanges;

    private RangeMap<StreamFromOptions> denormalizeInput;
    private Range<Token> denormalizeRange;

    private static Range<Token> r(long left, long right)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    @Setup(Level.Trial)
    public void setup()
    {
        // Non-wrapping overlapping
        nonWrapA = r(100, 500);
        nonWrapB = r(300, 700);

        // Non-wrapping disjoint
        nonWrapC = r(100, 200);
        nonWrapD = r(300, 400);

        // Wrapping + non-wrapping overlapping
        wrapA = r(800, 200);
        nonWrapInWrap = r(50, 150);

        // Wrapping + non-wrapping in gap
        nonWrapInGap = r(300, 500);

        // Two wrapping
        wrapB = r(500, 100);

        // Full ring
        fullRing = r(0, 0);
    }

    @Setup(Level.Invocation)
    public void setupDenormalize()
    {
        // Rebuild denormalize input each invocation to get fresh state
        denormalizeInput = new RangeMap<>();
        long stride = 1_000_000L / numExistingRanges;
        for (int i = 0; i < numExistingRanges; i++)
        {
            long start = i * stride;
            long end = start + stride / 2;
            Range<Token> range = r(start, end);
            denormalizeInput.put(range, new StreamFromOptions(null, range));
        }
        // Pick a range that overlaps one existing range in the middle
        long mid = (numExistingRanges / 2) * stride;
        denormalizeRange = r(mid + stride / 4, mid + stride / 4 + stride / 8);
    }

    // --- Range.intersects() benchmarks ---

    @Benchmark
    public boolean intersects_neitherWraps_hit()
    {
        return nonWrapA.intersects(nonWrapB);
    }

    @Benchmark
    public boolean intersects_neitherWraps_miss()
    {
        return nonWrapC.intersects(nonWrapD);
    }

    @Benchmark
    public boolean intersects_oneWraps_hit()
    {
        return wrapA.intersects(nonWrapInWrap);
    }

    @Benchmark
    public boolean intersects_oneWraps_miss()
    {
        return wrapA.intersects(nonWrapInGap);
    }

    @Benchmark
    public boolean intersects_bothWrap()
    {
        return wrapA.intersects(wrapB);
    }

    @Benchmark
    public boolean intersects_fullRing()
    {
        return fullRing.intersects(nonWrapA);
    }

    // --- RangeDenormalizer.denormalize() benchmark ---

    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void denormalize(Blackhole bh)
    {
        bh.consume(RangeDenormalizer.denormalize(denormalizeRange, denormalizeInput));
    }
}
