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
package org.apache.cassandra.dht;

import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.*;

/**
 * Tests for the allocation-free Range.intersects() implementation.
 * Verifies correctness against intersectionWith() for all wrap-around cases.
 */
public class RangeIntersectsTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private static Range<Token> r(long left, long right)
    {
        return new Range<>(t(left), t(right));
    }

    // ---- Neither wraps ----

    @Test
    public void testNeitherWraps_overlapping()
    {
        // (10, 50] and (30, 70] overlap at (30, 50]
        assertTrue(r(10, 50).intersects(r(30, 70)));
        assertTrue(r(30, 70).intersects(r(10, 50)));
    }

    @Test
    public void testNeitherWraps_disjoint()
    {
        // (10, 30] and (50, 70] are disjoint
        assertFalse(r(10, 30).intersects(r(50, 70)));
        assertFalse(r(50, 70).intersects(r(10, 30)));
    }

    @Test
    public void testNeitherWraps_adjacent()
    {
        // (10, 30] and (30, 50] touch at point 30 but don't intersect
        // because ranges are (left, right] - 30 is in the first but not the second's interior
        assertFalse(r(10, 30).intersects(r(30, 50)));
        assertFalse(r(30, 50).intersects(r(10, 30)));
    }

    @Test
    public void testNeitherWraps_contained()
    {
        // (10, 70] contains (20, 50]
        assertTrue(r(10, 70).intersects(r(20, 50)));
        assertTrue(r(20, 50).intersects(r(10, 70)));
    }

    @Test
    public void testNeitherWraps_identical()
    {
        assertTrue(r(10, 50).intersects(r(10, 50)));
    }

    @Test
    public void testNeitherWraps_sharedLeftEndpoint()
    {
        // (10, 30] and (10, 50] share left endpoint, overlap at (10, 30]
        assertTrue(r(10, 30).intersects(r(10, 50)));
        assertTrue(r(10, 50).intersects(r(10, 30)));
    }

    @Test
    public void testNeitherWraps_sharedRightEndpoint()
    {
        // (10, 50] and (30, 50] share right endpoint, overlap at (30, 50]
        assertTrue(r(10, 50).intersects(r(30, 50)));
        assertTrue(r(30, 50).intersects(r(10, 50)));
    }

    // ---- Both wrap ----

    @Test
    public void testBothWrap_alwaysIntersect()
    {
        // Two wrapping ranges that don't contain each other always intersect
        Range<Token> a = r(80, 20);  // wraps: covers (80, MAX] + [MIN, 20]
        Range<Token> b = r(50, 10);  // wraps: covers (50, MAX] + [MIN, 10]
        assertTrue(a.intersects(b));
        assertTrue(b.intersects(a));
    }

    @Test
    public void testBothWrap_oneContainsOther()
    {
        Range<Token> big = r(50, 30);   // wraps, larger
        Range<Token> small = r(80, 10); // wraps, smaller (contained by big)
        assertTrue(big.intersects(small));
        assertTrue(small.intersects(big));
    }

    @Test
    public void testBothWrap_identical()
    {
        Range<Token> a = r(80, 20);
        assertTrue(a.intersects(a));
    }

    // ---- One wraps, one doesn't ----

    @Test
    public void testOneWraps_intersectsLowSegment()
    {
        // wrapping (80, 20] covers (80, MAX] + [MIN, 20]
        // non-wrapping (5, 15] is in the low segment
        assertTrue(r(80, 20).intersects(r(5, 15)));
        assertTrue(r(5, 15).intersects(r(80, 20)));
    }

    @Test
    public void testOneWraps_intersectsHighSegment()
    {
        // wrapping (80, 20] and non-wrapping (85, 95]
        assertTrue(r(80, 20).intersects(r(85, 95)));
        assertTrue(r(85, 95).intersects(r(80, 20)));
    }

    @Test
    public void testOneWraps_inGap()
    {
        // wrapping (80, 20] has a gap at (20, 80]
        // non-wrapping (30, 70] falls entirely in that gap
        assertFalse(r(80, 20).intersects(r(30, 70)));
        assertFalse(r(30, 70).intersects(r(80, 20)));
    }

    @Test
    public void testOneWraps_adjacentAtRight()
    {
        // wrapping (80, 20] and non-wrapping (20, 50] - adjacent at point 20
        // 20 is in the wrapping range (right endpoint) but not in (20, 50] (left endpoint excluded)
        assertFalse(r(80, 20).intersects(r(20, 50)));
        assertFalse(r(20, 50).intersects(r(80, 20)));
    }

    @Test
    public void testOneWraps_adjacentAtLeft()
    {
        // wrapping (80, 20] and non-wrapping (50, 80] - adjacent at point 80
        // 80 is in (50, 80] but NOT in wrapping (80, 20] (left endpoint excluded)
        assertFalse(r(80, 20).intersects(r(50, 80)));
        assertFalse(r(50, 80).intersects(r(80, 20)));
    }

    @Test
    public void testOneWraps_wrappingLeftEqualsOtherRight()
    {
        // wrapping.left == other.right: wrapping (80, 20], other (50, 80]
        // Point 80 is not in wrapping range (left excluded). They don't share interior points.
        assertFalse(r(80, 20).intersects(r(50, 80)));
        assertFalse(r(50, 80).intersects(r(80, 20)));
    }

    // ---- Full ring ----

    @Test
    public void testFullRing_intersectsEverything()
    {
        Range<Token> full = r(0, 0); // full ring
        assertTrue(full.intersects(r(10, 50)));
        assertTrue(full.intersects(r(80, 20))); // wrapping
        assertTrue(full.intersects(r(0, 0)));   // another full ring
        assertTrue(r(10, 50).intersects(full));
        assertTrue(r(80, 20).intersects(full));
    }

    @Test
    public void testFullRing_differentStartPoints()
    {
        // Full ring at different start points
        Range<Token> full1 = r(0, 0);
        Range<Token> full2 = r(50, 50);
        Range<Token> full3 = r(-1, -1);
        assertTrue(full1.intersects(full2));
        assertTrue(full2.intersects(full3));
        assertTrue(full1.intersects(full3));
    }

    // ---- MIN/MAX token edge cases ----

    @Test
    public void testMinTokenEndpoint()
    {
        // Range ending at Long.MIN_VALUE (Murmur3 minimum)
        // (80, MIN] is wrapping per isWrapAround but unwrap() treats as single range
        long min = Long.MIN_VALUE;
        Range<Token> toMin = r(80, min);
        Range<Token> inRange = r(90, 100);
        Range<Token> outRange = r(10, 50);

        assertTrue(toMin.intersects(inRange));
        assertTrue(inRange.intersects(toMin));
        assertFalse(toMin.intersects(outRange));
        assertFalse(outRange.intersects(toMin));
    }

    @Test
    public void testMaxTokenEndpoint()
    {
        long max = Long.MAX_VALUE;
        Range<Token> toMax = r(10, max);
        Range<Token> inRange = r(50, 100);

        assertTrue(toMax.intersects(inRange));
        assertTrue(inRange.intersects(toMax));
    }

    @Test
    public void testMinMaxBoundary()
    {
        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;
        // (MAX-1, MIN+1] is a tiny wrapping range containing just MAX and MIN+1
        Range<Token> tiny = r(max - 1, min + 1);
        Range<Token> middle = r(10, 50);

        assertFalse(tiny.intersects(middle));
        assertFalse(middle.intersects(tiny));
    }

    // ---- Symmetry ----

    @Test
    public void testSymmetry()
    {
        // intersects must be symmetric: a.intersects(b) == b.intersects(a)
        Range<Token>[] ranges = new Range[] {
            r(10, 50), r(30, 70), r(80, 20), r(50, 10),
            r(0, 0), r(50, 50), r(Long.MIN_VALUE, Long.MAX_VALUE),
            r(Long.MAX_VALUE, Long.MIN_VALUE + 1)
        };

        for (Range<Token> a : ranges)
            for (Range<Token> b : ranges)
                assertEquals(String.format("Symmetry violated for %s and %s", a, b),
                             a.intersects(b), b.intersects(a));
    }

    // ---- Consistency with intersectionWith ----

    @Test
    public void testConsistencyWithIntersectionWith()
    {
        // intersects() must return true iff intersectionWith().size() > 0
        Range<Token>[] ranges = new Range[] {
            r(10, 50), r(30, 70), r(80, 20), r(50, 10),
            r(0, 0), r(50, 50), r(20, 80), r(10, 30),
            r(30, 50), r(70, 90), r(Long.MIN_VALUE + 1, Long.MAX_VALUE),
            r(Long.MAX_VALUE, Long.MIN_VALUE + 1)
        };

        for (Range<Token> a : ranges)
        {
            for (Range<Token> b : ranges)
            {
                boolean intersects = a.intersects(b);
                boolean hasIntersection = a.intersectionWith(b).size() > 0;
                assertEquals(String.format("Mismatch for %s and %s: intersects=%b, intersectionWith.size>0=%b",
                                           a, b, intersects, hasIntersection),
                             hasIntersection, intersects);
            }
        }
    }

    // ---- Fuzz test ----

    @Test
    public void testRandomConsistency()
    {
        // Fuzz test: random ranges, verify intersects() matches intersectionWith()
        Random rnd = new Random(42);
        for (int i = 0; i < 100_000; i++)
        {
            Range<Token> a = r(rnd.nextLong(), rnd.nextLong());
            Range<Token> b = r(rnd.nextLong(), rnd.nextLong());

            boolean fast = a.intersects(b);
            boolean reference = a.intersectionWith(b).size() > 0;

            assertEquals(String.format("Iteration %d: mismatch for %s and %s", i, a, b),
                         reference, fast);
            // symmetry
            assertEquals(String.format("Iteration %d: symmetry for %s and %s", i, a, b),
                         fast, b.intersects(a));
        }
    }
}
