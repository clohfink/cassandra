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

package com.netflix.cassandra.importing;

import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.Clock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ImportJobTimeoutTest
{
    /**
     * Mock Clock implementation that allows manual time advancement for testing.
     */
    private static class MockClock implements Clock
    {
        private long currentTime = 0;

        @Override
        public long nanoTime()
        {
            return currentTime * 1_000_000; // Convert ms to ns
        }

        @Override
        public long currentTimeMillis()
        {
            return currentTime;
        }

        public void advance(long milliseconds)
        {
            currentTime += milliseconds;
        }

        public void setTime(long milliseconds)
        {
            currentTime = milliseconds;
        }

        public void reset()
        {
            currentTime = 0;
        }
    }

    private static final MockClock mockClock = new MockClock();
    private Clock originalClock;

    @Before
    public void setUp()
    {
        // Save original clock and replace with mock
        originalClock = ImportJob.CLOCK;
        ImportJob.CLOCK = mockClock;
        mockClock.reset();
    }

    @After
    public void tearDown()
    {
        // Restore original clock
        ImportJob.CLOCK = originalClock;
    }

    /**
     * A step that never completes (always returns itself from checkComplete).
     */
    private static class NeverCompletingStep implements ImportStep
    {
        private final long timeout;
        private int checkCompleteCallCount = 0;
        private boolean shouldFailInit = false;

        public NeverCompletingStep(long timeout)
        {
            this.timeout = timeout;
        }

        public NeverCompletingStep(long timeout, boolean shouldFailInit)
        {
            this.timeout = timeout;
            this.shouldFailInit = shouldFailInit;
        }

        @Override
        public void init()
        {
            if (shouldFailInit)
            {
                throw new RuntimeException("Init failed for testing");
            }
            // Otherwise no-op
        }

        @Override
        public ImportStep checkComplete()
        {
            checkCompleteCallCount++;
            return this; // Never completes, always returns itself
        }

        @Override
        public ImportStatus getStatus()
        {
            return ImportStatus.VALIDATING;
        }

        @Override
        public double getProgress()
        {
            return 0.0;
        }

        @Override
        public Map<String, String> toStatusMap()
        {
            return baseStatusMap();
        }

        @Override
        public long timeoutMillis()
        {
            return timeout;
        }

        @Override
        public long getNextCheckDelayMs()
        {
            return Long.MAX_VALUE; // Don't reschedule automatically
        }

        public int getCheckCompleteCallCount()
        {
            return checkCompleteCallCount;
        }
    }

    /**
     * A step that transitions to another step after init.
     */
    private static class TransitioningStep implements ImportStep
    {
        private final ImportStep nextStep;
        private final long timeout;

        public TransitioningStep(ImportStep nextStep, long timeout)
        {
            this.nextStep = nextStep;
            this.timeout = timeout;
        }

        @Override
        public void init()
        {
            // No-op
        }

        @Override
        public ImportStep checkComplete()
        {
            return nextStep;
        }

        @Override
        public ImportStatus getStatus()
        {
            return ImportStatus.FILTERING;
        }

        @Override
        public double getProgress()
        {
            return 0.5;
        }

        @Override
        public Map<String, String> toStatusMap()
        {
            return baseStatusMap();
        }

        @Override
        public long timeoutMillis()
        {
            return timeout;
        }

        @Override
        public long getNextCheckDelayMs()
        {
            return Long.MAX_VALUE;
        }
    }

    @Test
    public void testStepTimeoutWithDefaultTimeout()
    {
        mockClock.setTime(1000);

        NeverCompletingStep step = new NeverCompletingStep(ImportJob.DEFAULT_TIMEOUT_MS);
        ImportJob job = new ImportJob(UUID.randomUUID(), "test_ks", "test_table", step);

        // Initially job should be in VALIDATING state
        assertEquals(ImportStatus.VALIDATING, job.status.get());

        // Advance time to just before timeout
        mockClock.advance(ImportJob.DEFAULT_TIMEOUT_MS - 1);
        job.checkState();

        // Should still be VALIDATING (not timed out yet)
        assertEquals(ImportStatus.VALIDATING, job.status.get());

        // Advance time past the timeout
        mockClock.advance(2);
        job.checkState();

        // Should now be in ERROR state due to timeout
        assertEquals(ImportStatus.ERROR, job.status.get());
        assertNotNull(job.errorMessage);
        assertTrue("Error message should mention timeout", job.errorMessage.contains("timed out"));
    }

    @Test
    public void testStepTimeoutWithCustomTimeout()
    {
        mockClock.setTime(0);

        long customTimeout = 5000; // 5 seconds
        NeverCompletingStep step = new NeverCompletingStep(customTimeout);
        ImportJob job = new ImportJob(UUID.randomUUID(), "test_ks", "test_table", step);

        assertEquals(ImportStatus.VALIDATING, job.status.get());

        // Advance time to just before custom timeout
        mockClock.advance(customTimeout - 1);
        job.checkState();

        // Should still be VALIDATING
        assertEquals(ImportStatus.VALIDATING, job.status.get());

        // Advance time past the custom timeout
        mockClock.advance(2);
        job.checkState();

        // Should now be in ERROR state
        assertEquals(ImportStatus.ERROR, job.status.get());
        assertNotNull(job.errorMessage);
        assertTrue("Error message should mention timeout", job.errorMessage.contains("timed out"));
        assertTrue("Error message should mention timeout duration",
                   job.errorMessage.contains(String.valueOf(customTimeout)));
    }

    @Test
    public void testTimeoutResetsOnStepTransition() throws Exception
    {
        mockClock.setTime(0);

        long timeout1 = 1000;
        long timeout2 = 2000;

        // Create a chain: step1 -> step2 (never completes)
        NeverCompletingStep step2 = new NeverCompletingStep(timeout2);
        TransitioningStep step1 = new TransitioningStep(step2, timeout1);
        ImportJob job = new ImportJob(UUID.randomUUID(), "test_ks", "test_table", step1);

        // Initial state should be FILTERING (step1)
        assertEquals(ImportStatus.FILTERING, job.status.get());

        // Advance time to just before step1's timeout
        mockClock.advance(timeout1 - 100);

        // Should still be FILTERING and not timed out
        assertEquals(ImportStatus.FILTERING, job.status.get());

        // Trigger transition to step2 by calling checkState()
        // This should happen before step1 times out
        mockClock.advance(50); // Still within step1's timeout
        job.checkState();

        // Should now be in VALIDATING state (step2)
        assertEquals(ImportStatus.VALIDATING, job.status.get());

        // Now advance time by step2's timeout amount from the transition point
        // The timer should have reset, so step1's elapsed time doesn't count
        mockClock.advance(timeout2 - 1);
        job.checkState();

        // Should still be VALIDATING (step2 not timed out yet)
        assertEquals(ImportStatus.VALIDATING, job.status.get());

        // Advance past step2's timeout
        mockClock.advance(2);
        job.checkState();

        // Now should be in ERROR state due to step2 timeout
        assertEquals(ImportStatus.ERROR, job.status.get());
        assertNotNull(job.errorMessage);
        assertTrue("Error message should mention timeout", job.errorMessage.contains("timed out"));
    }

    @Test
    public void testMultipleCheckStateCallsBeforeTimeout()
    {
        mockClock.setTime(0);

        long timeout = 10000;
        NeverCompletingStep step = new NeverCompletingStep(timeout);
        ImportJob job = new ImportJob(UUID.randomUUID(), "test_ks", "test_table", step);

        assertEquals(ImportStatus.VALIDATING, job.status.get());

        // Call checkState multiple times with small time advances
        for (int i = 0; i < 5; i++)
        {
            mockClock.advance(1000);
            job.checkState();
            assertEquals("Should still be VALIDATING on iteration " + i,
                        ImportStatus.VALIDATING, job.status.get());
        }

        // Total elapsed: 5000ms, still under 10000ms timeout
        assertEquals(5, step.getCheckCompleteCallCount());

        // Advance past timeout
        mockClock.advance(5001);
        job.checkState();

        // Should now timeout
        assertEquals(ImportStatus.ERROR, job.status.get());
        assertTrue(job.errorMessage.contains("timed out"));
    }

    /**
     * Test timeout in STAGED state while waiting for cluster coordination.
     * This simulates a scenario where nodes are stuck waiting in STAGED state
     * for other nodes to complete, and eventually timeout.
     */
    @Test
    public void testStagedStateTimeout()
    {
        mockClock.setTime(0);

        long stagedTimeout = 15000; // 15 seconds

        // Create a step that simulates being stuck in STAGED state
        ImportStep stagedStep = new ImportStep()
        {
            private int checkCount = 0;

            @Override
            public ImportStep checkComplete()
            {
                checkCount++;
                // Never completes - simulates waiting for other nodes
                return this;
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.STAGED;
            }

            @Override
            public double getProgress()
            {
                return 0.6; // 60% progress, stuck waiting
            }

            @Override
            public Map<String, String> toStatusMap()
            {
                Map<String, String> status = baseStatusMap();
                status.put("message", "Waiting for other nodes to reach STAGED state");
                status.put("checks_performed", String.valueOf(checkCount));
                return status;
            }

            @Override
            public long timeoutMillis()
            {
                return stagedTimeout;
            }

            @Override
            public long getNextCheckDelayMs()
            {
                return 1000; // Check every second
            }
        };

        ImportJob job = new ImportJob(UUID.randomUUID(), "test_ks", "test_table", stagedStep);

        // Initial state should be STAGED
        assertEquals(ImportStatus.STAGED, job.status.get());

        // Advance time and check state multiple times (simulating waiting)
        for (int i = 0; i < 10; i++)
        {
            mockClock.advance(1000);
            job.checkState();
            assertEquals("Should still be STAGED on check " + i,
                        ImportStatus.STAGED, job.status.get());
        }

        // Total elapsed: 10 seconds, still under 15 second timeout
        assertEquals(ImportStatus.STAGED, job.status.get());

        // Advance past the STAGED timeout
        mockClock.advance(5500);
        job.checkState();

        // Should now be in ERROR state due to STAGED timeout
        assertEquals(ImportStatus.ERROR, job.status.get());
        assertNotNull("Should have error message", job.errorMessage);
        assertTrue("Error message should mention timeout",
                   job.errorMessage.contains("timed out"));
        assertTrue("Error message should reference STAGED state or waiting",
                   job.errorMessage.toLowerCase().contains("staged") ||
                   job.errorMessage.toLowerCase().contains("waiting"));
    }
}
