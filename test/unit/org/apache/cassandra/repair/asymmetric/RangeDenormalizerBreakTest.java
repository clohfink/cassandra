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
package org.apache.cassandra.repair.asymmetric;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.repair.asymmetric.ReduceHelperTest.range;

/**
 * Tests for the break optimisation in RangeDenormalizer.denormalize().
 * Ensures the early-exit doesn't change results with many non-overlapping ranges.
 */
public class RangeDenormalizerBreakTest
{
    @Test
    public void testManyNonOverlappingRanges_newInMiddle()
    {
        // Many non-overlapping ranges in the map; new range overlaps exactly one
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        for (int i = 0; i < 100; i++)
            incoming.put(range(i * 100, i * 100 + 50), new StreamFromOptions(null, range(i * 100, i * 100 + 50)));

        // New range overlaps only (500, 550] → splits it
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(520, 540), incoming);

        assertEquals(102, incoming.size()); // one split into 3, others untouched: 100 - 1 + 3 = 102
        assertTrue(incoming.containsKey(range(500, 520)));
        assertTrue(incoming.containsKey(range(520, 540)));
        assertTrue(incoming.containsKey(range(540, 550)));
        assertEquals(1, newInput.size());
        assertTrue(newInput.contains(range(520, 540)));
    }

    @Test
    public void testManyNonOverlappingRanges_newSpansGap()
    {
        // New range spans a gap between existing ranges
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(0, 100), new StreamFromOptions(null, range(0, 100)));
        incoming.put(range(200, 300), new StreamFromOptions(null, range(200, 300)));
        incoming.put(range(400, 500), new StreamFromOptions(null, range(400, 500)));

        // New range (50, 250] overlaps (0,100] and (200,300]
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(50, 250), incoming);

        // incoming should have: (0,50], (50,100], (200,250], (250,300], (400,500]
        assertEquals(5, incoming.size());
        assertTrue(incoming.containsKey(range(0, 50)));
        assertTrue(incoming.containsKey(range(50, 100)));
        assertTrue(incoming.containsKey(range(200, 250)));
        assertTrue(incoming.containsKey(range(250, 300)));
        assertTrue(incoming.containsKey(range(400, 500)));

        // newInput should have: (50,100], (100,200], (200,250]
        Set<Range<Token>> expected = Sets.newHashSet(range(50, 100), range(100, 200), range(200, 250));
        assertEquals(expected, newInput);
    }

    @Test
    public void testManyNonOverlappingRanges_newDisjoint()
    {
        // New range doesn't overlap any existing range
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(0, 100), new StreamFromOptions(null, range(0, 100)));
        incoming.put(range(200, 300), new StreamFromOptions(null, range(200, 300)));

        // (150, 180] is in the gap
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(150, 180), incoming);

        // Existing ranges unchanged
        assertEquals(2, incoming.size());
        assertTrue(incoming.containsKey(range(0, 100)));
        assertTrue(incoming.containsKey(range(200, 300)));

        // newInput is just the new range itself
        assertEquals(1, newInput.size());
        assertTrue(newInput.contains(range(150, 180)));
    }

    @Test
    public void testRepeatedDenormalize_growingMap()
    {
        // Simulate the repair scenario: repeatedly denormalize into a growing map
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(0, 1000), new StreamFromOptions(null, range(0, 1000)));

        // First denormalize splits (0, 1000] around (100, 200]
        RangeDenormalizer.denormalize(range(100, 200), incoming);
        assertEquals(3, incoming.size());
        assertTrue(incoming.containsKey(range(0, 100)));
        assertTrue(incoming.containsKey(range(100, 200)));
        assertTrue(incoming.containsKey(range(200, 1000)));

        // Second denormalize splits (200, 1000] around (300, 400]
        RangeDenormalizer.denormalize(range(300, 400), incoming);
        assertEquals(5, incoming.size());
        assertTrue(incoming.containsKey(range(0, 100)));
        assertTrue(incoming.containsKey(range(100, 200)));
        assertTrue(incoming.containsKey(range(200, 300)));
        assertTrue(incoming.containsKey(range(300, 400)));
        assertTrue(incoming.containsKey(range(400, 1000)));

        // Third denormalize overlaps two existing ranges: (150, 350]
        RangeDenormalizer.denormalize(range(150, 350), incoming);
        assertEquals(7, incoming.size());
        assertTrue(incoming.containsKey(range(0, 100)));
        assertTrue(incoming.containsKey(range(100, 150)));
        assertTrue(incoming.containsKey(range(150, 200)));
        assertTrue(incoming.containsKey(range(200, 300)));
        assertTrue(incoming.containsKey(range(300, 350)));
        assertTrue(incoming.containsKey(range(350, 400)));
        assertTrue(incoming.containsKey(range(400, 1000)));
    }

    @Test
    public void testDenormalize_existingRangeFullyContainsNew()
    {
        // Single existing range fully contains the new one
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(0, 1000), new StreamFromOptions(null, range(0, 1000)));

        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(400, 600), incoming);

        assertEquals(3, incoming.size());
        assertTrue(incoming.containsKey(range(0, 400)));
        assertTrue(incoming.containsKey(range(400, 600)));
        assertTrue(incoming.containsKey(range(600, 1000)));
        assertEquals(1, newInput.size());
        assertTrue(newInput.contains(range(400, 600)));
    }

    @Test
    public void testDenormalize_newFullyContainsExisting()
    {
        // New range fully contains an existing range
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        incoming.put(range(400, 600), new StreamFromOptions(null, range(400, 600)));

        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(0, 1000), incoming);

        // incoming keeps the original range
        assertEquals(1, incoming.size());
        assertTrue(incoming.containsKey(range(400, 600)));

        // newInput has the original plus the two subtracted parts
        assertEquals(3, newInput.size());
        assertTrue(newInput.contains(range(0, 400)));
        assertTrue(newInput.contains(range(400, 600)));
        assertTrue(newInput.contains(range(600, 1000)));
    }

    @Test
    public void testDenormalize_manySmallRanges()
    {
        // Stress test: 500 small ranges, denormalize with a range spanning many of them
        RangeMap<StreamFromOptions> incoming = new RangeMap<>();
        int n = 500;
        for (int i = 0; i < n; i++)
            incoming.put(range(i * 10, i * 10 + 5), new StreamFromOptions(null, range(i * 10, i * 10 + 5)));

        assertEquals(n, incoming.size());

        // New range overlaps ranges at 100-105, 110-115, ..., 190-195 (10 ranges)
        Set<Range<Token>> newInput = RangeDenormalizer.denormalize(range(102, 193), incoming);

        // Verify the ranges that were split are present
        assertTrue(incoming.containsKey(range(100, 102)));
        assertTrue(incoming.containsKey(range(102, 105)));
        assertTrue(incoming.containsKey(range(190, 193)));
        assertTrue(incoming.containsKey(range(193, 195)));

        // Ranges outside the denormalized area should be untouched
        assertTrue(incoming.containsKey(range(0, 5)));
        assertTrue(incoming.containsKey(range(200, 205)));
        assertTrue(incoming.containsKey(range(4990, 4995)));
    }
}
