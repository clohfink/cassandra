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

package com.netflix.cassandra.importing.steps;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;

import static org.junit.Assert.*;

/**
 * Tests for TrimStep retry logic when cleanup is interrupted by compaction (e.g. during repair).
 */
public class TrimStepRetryTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        // Use small delays for fast tests
        DatabaseDescriptor.setImportCleanupMaxRetries(3);
        DatabaseDescriptor.setImportCleanupRetryInitialDelaySeconds(0);
    }

    /**
     * Creates a TrimStep that delegates performCleanup to the given Runnable.
     */
    private TrimStep createTrimStep(CleanupAction action)
    {
        return new TrimStep(UUID.randomUUID(), "test_ks", "test_table")
        {
            @Override
            protected void performCleanup() throws ExecutionException, InterruptedException
            {
                action.run();
            }
        };
    }

    @FunctionalInterface
    interface CleanupAction
    {
        void run() throws ExecutionException, InterruptedException;
    }

    @Test
    public void testSuccessOnFirstAttempt()
    {
        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(attempts::incrementAndGet);

        step.init();

        assertEquals(1, attempts.get());
        assertEquals(1.0, step.getProgress(), 0.0);
    }

    @Test
    public void testRetryOnCompactionInterrupted()
    {
        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(() -> {
            if (attempts.incrementAndGet() < 3)
                throw new CompactionInterruptedException("test");
        });

        step.init();

        assertEquals(3, attempts.get());
        assertEquals(1.0, step.getProgress(), 0.0);
    }

    @Test
    public void testRetryOnWrappedCompactionInterrupted()
    {
        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(() -> {
            if (attempts.incrementAndGet() < 2)
                throw new ExecutionException(new CompactionInterruptedException("test"));
        });

        step.init();

        assertEquals(2, attempts.get());
    }

    @Test
    public void testExhaustedRetriesThrows()
    {
        DatabaseDescriptor.setImportCleanupMaxRetries(2);

        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(() -> {
            attempts.incrementAndGet();
            throw new CompactionInterruptedException("always interrupted");
        });

        try
        {
            step.init();
            fail("Should throw after exhausting retries");
        }
        catch (RuntimeException e)
        {
            // CompactionInterruptedException is a RuntimeException, caught by outer catch and wrapped
            assertNotNull(TrimStep.findCompactionInterruptedException(e));
        }

        // 1 initial + 2 retries = 3
        assertEquals(3, attempts.get());
        assertEquals(0.0, step.getProgress(), 0.0);
    }

    @Test
    public void testNonCompactionExceptionNotRetried()
    {
        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("some other error");
        });

        try
        {
            step.init();
            fail("Should throw immediately");
        }
        catch (RuntimeException e)
        {
            assertEquals("some other error", e.getMessage());
        }

        assertEquals(1, attempts.get());
    }

    @Test
    public void testWrappedNonCompactionExceptionNotRetried()
    {
        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(() -> {
            attempts.incrementAndGet();
            throw new ExecutionException(new RuntimeException("not compaction related"));
        });

        try
        {
            step.init();
            fail("Should throw immediately");
        }
        catch (RuntimeException e)
        {
            // ExecutionException caught by outer catch, wrapped in RuntimeException
            assertTrue(e.getCause() instanceof ExecutionException);
        }

        assertEquals(1, attempts.get());
    }

    @Test
    public void testInterruptedExceptionNotRetried()
    {
        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(() -> {
            attempts.incrementAndGet();
            throw new InterruptedException("thread interrupted");
        });

        try
        {
            step.init();
            fail("Should throw immediately");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getCause() instanceof InterruptedException);
        }

        assertEquals(1, attempts.get());
    }

    @Test
    public void testZeroRetriesMeansOneAttempt()
    {
        DatabaseDescriptor.setImportCleanupMaxRetries(0);

        AtomicInteger attempts = new AtomicInteger(0);
        TrimStep step = createTrimStep(() -> {
            attempts.incrementAndGet();
            throw new CompactionInterruptedException("interrupted");
        });

        try
        {
            step.init();
            fail("Should throw");
        }
        catch (RuntimeException e)
        {
            assertNotNull(TrimStep.findCompactionInterruptedException(e));
        }

        assertEquals(1, attempts.get());
    }

    // findCompactionInterruptedException tests

    @Test
    public void testFindDirectCompactionInterrupted()
    {
        CompactionInterruptedException cie = new CompactionInterruptedException("test");
        assertSame(cie, TrimStep.findCompactionInterruptedException(cie));
    }

    @Test
    public void testFindWrappedCompactionInterrupted()
    {
        CompactionInterruptedException cie = new CompactionInterruptedException("test");
        ExecutionException wrapper = new ExecutionException(cie);
        assertSame(cie, TrimStep.findCompactionInterruptedException(wrapper));
    }

    @Test
    public void testFindDeeplyWrappedCompactionInterrupted()
    {
        CompactionInterruptedException cie = new CompactionInterruptedException("test");
        Exception wrapper = new ExecutionException(new RuntimeException(cie));
        assertSame(cie, TrimStep.findCompactionInterruptedException(wrapper));
    }

    @Test
    public void testFindReturnsNullForUnrelatedExceptions()
    {
        assertNull(TrimStep.findCompactionInterruptedException(new RuntimeException("nope")));
        assertNull(TrimStep.findCompactionInterruptedException(new ExecutionException(new IOException())));
        assertNull(TrimStep.findCompactionInterruptedException(null));
    }
}
