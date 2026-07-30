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
package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * JUnit face of {@link LargeSSTableSplitBench}. Runs the exact harness the shipped jar runs -- corpus
 * generation with the sorted writer, hard-link materialisation, page-cache eviction, both split paths, key
 * verification -- just at a size that finishes inside the long-test timeout.
 *
 * <p>It is <b>opt in</b>. A default {@code ant long-test} run should not spend minutes and gigabytes here, and
 * the interesting sizes are far past what any CI box should be asked to do, so
 * {@link #benchmarkLargeParent()} skips unless {@code -Dcassandra.test.zerocopysplit.largescale=true} is set:
 * <pre>
 *   ant long-testsome -Duse.jdk11=true \
 *       -Dtest.name=org.apache.cassandra.io.sstable.LargeSSTableSplitBenchTest \
 *       -Dtest.jvm.args="-Xmx4G -Dcassandra.test.zerocopysplit.largescale=true"
 * </pre>
 * Size and shape are overridable the same way, e.g.
 * {@code -Dcassandra.test.zerocopysplit.largescale.size=8GiB}. Anything past a few GiB will outrun
 * {@code test.long.timeout} (10 minutes); at that point use the jar, which is what it is for.
 *
 * <p>{@link #murmur3InverseGivesSortedKeys()} always runs. It is cheap and it guards the one assumption the
 * whole corpus generator rests on: that walking the token ring in even steps and inverting murmur3 yields
 * partition keys in strictly increasing decorated-key order, so a terabyte can be written by a sorted writer
 * that buffers nothing.
 */
public class LargeSSTableSplitBenchTest
{
    private static final String PROP_ENABLED = "cassandra.test.zerocopysplit.largescale";
    private static final String PROP_SIZE = "cassandra.test.zerocopysplit.largescale.size";
    private static final String PROP_CHILDREN = "cassandra.test.zerocopysplit.largescale.children";

    /**
     * Small enough to build, split twice and verify inside {@code test.long.timeout}, big enough that the
     * parent spans thousands of compression chunks and every interesting case in the splitter (dead prefixes,
     * boundary chunks shared by two children) actually occurs.
     */
    private static final String DEFAULT_SIZE = "512MiB";

    @Test
    public void benchmarkLargeParent() throws Throwable
    {
        Assume.assumeTrue("opt in with -D" + PROP_ENABLED + "=true",
                          Boolean.parseBoolean(System.getProperty(PROP_ENABLED, "false")));

        LargeSSTableSplitBench.Options options = new LargeSSTableSplitBench.Options();
        options.scratch = new File("build/test/zerocopy-split-largescale");
        options.targetBytes = LargeSSTableSplitBench.Options.parseBytes(System.getProperty(PROP_SIZE, DEFAULT_SIZE));
        options.children = Integer.getInteger(PROP_CHILDREN, 4);
        options.partitionSize = 64 * 1024;
        options.valueSize = 4 * 1024;
        // Both paths must survive a full scan of everything they produced, not just an Index.db fingerprint.
        // Affordable at this size; the jar defaults to keys-only because it is not affordable at a terabyte.
        options.verify = "rows";
        options.resolveDefaults();

        try (LargeSSTableSplitBench.Report report = LargeSSTableSplitBench.Report.open(options.reportFile))
        {
            // The harness asserts the invariants itself -- both paths' children must carry exactly the parent's
            // partition keys and exactly its row count -- and throws if either is violated.
            new LargeSSTableSplitBench(options, report).run();
        }
    }

    /**
     * The corpus generator never sorts and never holds the key set: it walks the token ring in even steps and
     * runs murmur3 backwards ({@link Murmur3Partitioner.LongToken#keyForToken(long)}) to get a key for each
     * token. That is only sound if the resulting keys come out in strictly increasing decorated-key order, and
     * if each one really does hash back to the token it was asked for -- otherwise a multi-hour generation
     * dies partway through on the sorted writer's ordering check.
     */
    @Test
    public void murmur3InverseGivesSortedKeys()
    {
        Murmur3Partitioner partitioner = Murmur3Partitioner.instance;

        for (long partitions : new long[]{ 2, 3, 1024, 1_000_003, 1L << 32 })
        {
            long stride = Long.divideUnsigned(-1L, partitions);
            ByteBuffer previous = null;
            long previousToken = 0;

            // Walk the head, the tail and a stripe through the middle: enough to catch a sign or wraparound
            // bug without hashing four billion keys.
            for (long i : samples(partitions))
            {
                long token = Long.MIN_VALUE + 1 + i * stride;
                ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);

                assertEquals("keyForToken(" + token + ") does not hash back to its token",
                             token, partitioner.getToken(key).token);

                if (previous != null)
                {
                    assertTrue("tokens are not strictly increasing at i=" + i + " of " + partitions
                               + ": " + previousToken + " -> " + token,
                               token > previousToken);
                    assertTrue("decorated keys are not strictly increasing at i=" + i + " of " + partitions,
                               partitioner.decorateKey(key).compareTo(partitioner.decorateKey(previous)) > 0);
                }
                previous = key;
                previousToken = token;
            }
        }
    }

    private static long[] samples(long partitions)
    {
        if (partitions <= 4096)
        {
            long[] all = new long[(int) partitions];
            for (int i = 0; i < all.length; i++)
                all[i] = i;
            return all;
        }
        long[] samples = new long[3072];
        int at = 0;
        for (long i = 0; i < 1024; i++)
            samples[at++] = i;
        for (long i = 0; i < 1024; i++)
            samples[at++] = partitions / 2 + i;
        for (long i = partitions - 1024; i < partitions; i++)
            samples[at++] = i;
        return samples;
    }
}
