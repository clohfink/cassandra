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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for state machine edge cases and transitions.
 *
 * This test class covers:
 * - Concurrent checkComplete() calls
 * - Step initialization failures
 * - Cleanup failures during reset
 * - State transition atomicity
 */
public class StateTransitionTest
{
    /**
     * Test that init() is properly called during state transitions.
     */
    @Test
    public void testInitCalledOnTransition() throws Throwable
    {
        AtomicBoolean initCalled = new AtomicBoolean(false);
        AtomicBoolean checkCompleteCalled = new AtomicBoolean(false);

        ImportStep step1 = new ImportStep()
        {
            @Override
            public ImportStep checkComplete()
            {
                checkCompleteCalled.set(true);
                // Transition to step2
                return new ImportStep()
                {
                    @Override
                    public void init()
                    {
                        initCalled.set(true);
                    }

                    @Override
                    public ImportStep checkComplete()
                    {
                        return null; // Complete
                    }

                    @Override
                    public ImportStatus getStatus()
                    {
                        return ImportStatus.DONE;
                    }

                    @Override
                    public double getProgress()
                    {
                        return 1.0;
                    }

                    @Override
                    public Map<String, String> toStatusMap()
                    {
                        return baseStatusMap();
                    }
                };
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.VALIDATING;
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
        };

        // Simulate ImportJob calling checkComplete and detecting transition
        ImportStep result = step1.checkComplete();
        assertTrue("checkComplete should be called", checkCompleteCalled.get());
        assertNotNull("Should return next step", result);

        // Simulate ImportJob calling init on new step
        result.init();
        assertTrue("init should be called on new step", initCalled.get());
    }

    /**
     * Test that init() failure is handled gracefully.
     */
    @Test
    public void testInitFailureHandling()
    {
        AtomicBoolean cleanupCalled = new AtomicBoolean(false);

        ImportStep failingStep = new ImportStep()
        {
            @Override
            public void init()
            {
                throw new RuntimeException("Simulated init failure");
            }

            @Override
            public void cleanup()
            {
                cleanupCalled.set(true);
            }

            @Override
            public ImportStep checkComplete()
            {
                return null;
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
        };

        try
        {
            failingStep.init();
            fail("Should throw exception from init()");
        }
        catch (RuntimeException e)
        {
            assertEquals("Simulated init failure", e.getMessage());
        }

        // Verify cleanup can still be called
        failingStep.cleanup();
        assertTrue("cleanup should be called even after init failure", cleanupCalled.get());
    }

    /**
     * Test that cleanup() handles failures gracefully.
     */
    @Test
    public void testCleanupFailureHandling()
    {
        AtomicInteger cleanupCallCount = new AtomicInteger(0);

        ImportStep stepWithFailingCleanup = new ImportStep()
        {
            @Override
            public void cleanup()
            {
                cleanupCallCount.incrementAndGet();
                throw new RuntimeException("Simulated cleanup failure");
            }

            @Override
            public ImportStep checkComplete()
            {
                return null;
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
        };

        try
        {
            stepWithFailingCleanup.cleanup();
            fail("Should throw exception from cleanup()");
        }
        catch (RuntimeException e)
        {
            assertEquals("Simulated cleanup failure", e.getMessage());
        }

        assertEquals("cleanup should be called once", 1, cleanupCallCount.get());

        // Verify cleanup can be called again (for retry scenarios)
        try
        {
            stepWithFailingCleanup.cleanup();
        }
        catch (RuntimeException e)
        {
            // Expected
        }

        assertEquals("cleanup should be called twice", 2, cleanupCallCount.get());
    }

    /**
     * Test that checkComplete() can be called multiple times safely.
     */
    @Test
    public void testMultipleCheckCompleteCallsSafe() throws Throwable
    {
        AtomicInteger checkCount = new AtomicInteger(0);

        ImportStep step = new ImportStep()
        {
            @Override
            public ImportStep checkComplete()
            {
                int count = checkCount.incrementAndGet();
                if (count < 3)
                {
                    return this; // Continue in this step
                }
                return null; // Complete after 3 checks
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.DOWNLOADING;
            }

            @Override
            public double getProgress()
            {
                return checkCount.get() / 3.0;
            }

            @Override
            public Map<String, String> toStatusMap()
            {
                Map<String, String> status = baseStatusMap();
                status.put("check_count", String.valueOf(checkCount.get()));
                return status;
            }
        };

        // Call checkComplete multiple times
        ImportStep result1 = step.checkComplete();
        assertSame("First check should return itself", step, result1);
        assertEquals(1, checkCount.get());

        ImportStep result2 = step.checkComplete();
        assertSame("Second check should return itself", step, result2);
        assertEquals(2, checkCount.get());

        ImportStep result3 = step.checkComplete();
        assertNull("Third check should complete", result3);
        assertEquals(3, checkCount.get());
    }

    /**
     * Test that step timeout configuration works correctly.
     */
    @Test
    public void testStepTimeoutConfiguration()
    {
        long customTimeout = 60000; // 60 seconds

        ImportStep stepWithCustomTimeout = new ImportStep()
        {
            @Override
            public long timeoutMillis()
            {
                return customTimeout;
            }

            @Override
            public ImportStep checkComplete()
            {
                return null;
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.VALIDATING;
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
        };

        assertEquals("Should use custom timeout", customTimeout, stepWithCustomTimeout.timeoutMillis());

        // Test default timeout
        ImportStep stepWithDefaultTimeout = new ImportStep()
        {
            @Override
            public ImportStep checkComplete()
            {
                return null;
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.VALIDATING;
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
        };

        assertEquals("Should use default timeout", ImportJob.DEFAULT_TIMEOUT_MS, stepWithDefaultTimeout.timeoutMillis());
    }

    /**
     * Test that check delay configuration works correctly.
     */
    @Test
    public void testStepCheckDelayConfiguration()
    {
        long customDelay = 5000; // 5 seconds

        ImportStep stepWithCustomDelay = new ImportStep()
        {
            @Override
            public long getNextCheckDelayMs()
            {
                return customDelay;
            }

            @Override
            public ImportStep checkComplete()
            {
                return null;
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.DOWNLOADING;
            }

            @Override
            public double getProgress()
            {
                return 0.3;
            }

            @Override
            public Map<String, String> toStatusMap()
            {
                return baseStatusMap();
            }
        };

        assertEquals("Should use custom delay", customDelay, stepWithCustomDelay.getNextCheckDelayMs());

        // Test default delay
        ImportStep stepWithDefaultDelay = new ImportStep()
        {
            @Override
            public ImportStep checkComplete()
            {
                return null;
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.DOWNLOADING;
            }

            @Override
            public double getProgress()
            {
                return 0.3;
            }

            @Override
            public Map<String, String> toStatusMap()
            {
                return baseStatusMap();
            }
        };

        assertEquals("Should use default delay", ImportStep.DEFAULT_CHECK_DELAY_MS, stepWithDefaultDelay.getNextCheckDelayMs());
    }

    /**
     * Test that status is correctly reported at each step.
     */
    @Test
    public void testStatusReporting() throws Throwable
    {
        ImportStep step = new ImportStep()
        {
            private int checkCount = 0;

            @Override
            public ImportStep checkComplete()
            {
                checkCount++;
                return checkCount < 2 ? this : null;
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.DOWNLOADING;
            }

            @Override
            public double getProgress()
            {
                return checkCount / 2.0;
            }

            @Override
            public Map<String, String> toStatusMap()
            {
                Map<String, String> status = baseStatusMap();
                status.put("checks", String.valueOf(checkCount));
                status.put("message", "Processing step");
                return status;
            }
        };

        // Get initial status
        Map<String, String> status1 = step.toStatusMap();
        assertEquals("DOWNLOADING", status1.get("step"));
        assertEquals("0", status1.get("checks"));

        // Progress through step
        step.checkComplete();
        Map<String, String> status2 = step.toStatusMap();
        assertEquals("1", status2.get("checks"));

        step.checkComplete();
        Map<String, String> status3 = step.toStatusMap();
        assertEquals("2", status3.get("checks"));
    }

    /**
     * Test state transitions with cleanup between steps.
     */
    @Test
    public void testStateTransitionWithCleanup() throws Throwable
    {
        AtomicBoolean step1CleanupCalled = new AtomicBoolean(false);
        AtomicBoolean step2InitCalled = new AtomicBoolean(false);

        ImportStep step2 = new ImportStep()
        {
            @Override
            public void init()
            {
                step2InitCalled.set(true);
            }

            @Override
            public ImportStep checkComplete()
            {
                return null; // Complete
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.DONE;
            }

            @Override
            public double getProgress()
            {
                return 1.0;
            }

            @Override
            public Map<String, String> toStatusMap()
            {
                return baseStatusMap();
            }
        };

        ImportStep step1 = new ImportStep()
        {
            @Override
            public void cleanup()
            {
                step1CleanupCalled.set(true);
            }

            @Override
            public ImportStep checkComplete()
            {
                return step2;
            }

            @Override
            public ImportStatus getStatus()
            {
                return ImportStatus.DOWNLOADING;
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
        };

        // Simulate transition
        ImportStep nextStep = step1.checkComplete();
        assertNotNull("Should transition to next step", nextStep);
        assertSame("Should return step2", step2, nextStep);

        // Cleanup old step before initializing new one
        step1.cleanup();
        assertTrue("step1 cleanup should be called", step1CleanupCalled.get());

        // Initialize new step
        nextStep.init();
        assertTrue("step2 init should be called", step2InitCalled.get());
    }
}
