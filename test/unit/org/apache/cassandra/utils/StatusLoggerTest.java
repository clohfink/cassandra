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

package org.apache.cassandra.utils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.cql3.CQLTester;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.groupingBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatusLoggerTest extends CQLTester
{
    private static final Logger log = LoggerFactory.getLogger(StatusLoggerTest.class);

    @Test
    public void testStatusLoggerPrintsStatusOnlyOnceWhenInvokedConcurrently() throws Exception
    {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(StatusLogger.class);
        InMemoryAppender inMemoryAppender = new InMemoryAppender();
        logger.addAppender(inMemoryAppender);
        logger.setLevel(Level.TRACE);
        try
        {
            submitTwoLogRequestsConcurrently();
            verifyOnlySingleStatusWasAppendedConcurrently(inMemoryAppender.events);
        }
        finally
        {
            assertTrue("Could not remove in memory appender", logger.detachAppender(inMemoryAppender));
        }
    }

    private void submitTwoLogRequestsConcurrently() throws InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(StatusLogger::log);
        executorService.submit(StatusLogger::log);
        executorService.shutdown();
        Assert.assertTrue(executorService.awaitTermination(1, TimeUnit.MINUTES));
    }

    private void verifyOnlySingleStatusWasAppendedConcurrently(List<ILoggingEvent> events)
    {
        Map<String, List<ILoggingEvent>> eventsByThread = events.stream().collect(groupingBy(ILoggingEvent::getThreadName));
        List<String> threadNames = newArrayList(eventsByThread.keySet());

        assertEquals("Expected events from 2 threads only", 2, threadNames.size());

        List<ILoggingEvent> firstThreadEvents = eventsByThread.get(threadNames.get(0));
        List<ILoggingEvent> secondThreadEvents = eventsByThread.get(threadNames.get(1));

        assertTrue("Expected at least one event from the first thread", firstThreadEvents.size() >= 1);
        assertTrue("Expected at least one event from the second thread", secondThreadEvents.size() >= 1);

        if (areDisjunctive(firstThreadEvents, secondThreadEvents))
        {
            log.debug("Event time ranges are disjunctive - log invocations were made one after another");
        }
        else
        {
            verifyStatusWasPrintedAndBusyEventOccured(firstThreadEvents, secondThreadEvents);
        }
    }

    private boolean areDisjunctive(List<ILoggingEvent> firstThreadEvents, List<ILoggingEvent> secondThreadEvents)
    {
        Range<Long> firstThreadTimeRange = timestampsRange(firstThreadEvents);
        Range<Long> secondThreadTimeRange = timestampsRange(secondThreadEvents);
        boolean connected = firstThreadTimeRange.isConnected(secondThreadTimeRange);
        boolean disjunctive = !connected || firstThreadTimeRange.intersection(secondThreadTimeRange).isEmpty();
        log.debug("Time ranges {}, {}, disjunctive={}", firstThreadTimeRange, secondThreadTimeRange, disjunctive);
        return disjunctive;
    }

    private Range<Long> timestampsRange(List<ILoggingEvent> events)
    {
        List<Long> timestamps = events.stream().map(ILoggingEvent::getTimeStamp).collect(Collectors.toList());
        Long min = timestamps.stream().min(Comparator.naturalOrder()).get();
        Long max = timestamps.stream().max(Comparator.naturalOrder()).get();
        // It's open on one side to cover a case when second status starts printing at the same timestamp that previous one was finished
        return Range.closedOpen(min, max);
    }

    private void verifyStatusWasPrintedAndBusyEventOccured(List<ILoggingEvent> firstThreadEvents, List<ILoggingEvent> secondThreadEvents)
    {
        if (firstThreadEvents.size() > 1 && secondThreadEvents.size() > 1)
        {
            log.error("Both event lists contain more than one entry. First = {}, Second = {}", firstThreadEvents, secondThreadEvents);
            fail("More that one status log was appended concurrently");
        }
        else if (firstThreadEvents.size() <= 1 && secondThreadEvents.size() <= 1)
        {
            log.error("No status log was recorded. First = {}, Second = {}", firstThreadEvents, secondThreadEvents);
            fail("Status log was not appended");
        }
        else
        {
            log.info("Checking if logger was busy. First = {}, Second = {}", firstThreadEvents, secondThreadEvents);
            assertTrue("One 'logger busy' entry was expected",
                       isLoggerBusyTheOnlyEvent(firstThreadEvents) || isLoggerBusyTheOnlyEvent(secondThreadEvents));
        }
    }

    private boolean isLoggerBusyTheOnlyEvent(List<ILoggingEvent> events)
    {
        return events.size() == 1 &&
               events.get(0).getMessage().equals("StatusLogger is busy") &&
               events.get(0).getLevel() == Level.TRACE;
    }

    private static class InMemoryAppender extends AppenderBase<ILoggingEvent>
    {
        private final List<ILoggingEvent> events = newArrayList();

        private InMemoryAppender()
        {
            start();
        }

        @Override
        protected synchronized void append(ILoggingEvent event)
        {
            events.add(event);
        }
    }

    @Test
    public void getTasksByStageGroupsTasksCorrectly()
    {
        List<DebuggableTask.RunningDebuggableTask> tasks = newArrayList(
        createTask("thread-1-1", 0, 0),
        createTask("thread-1-2", 0, 0),
        createTask("thread-2-1", 0, 0),
        createTask("thread-2-2", 0, 0),
        createTask("thread-3", 0, 0)
        );

        Map<String, List<DebuggableTask.RunningDebuggableTask>> groupedTasks = StatusLogger.getTasksByStage(tasks);
        assertEquals(3, groupedTasks.size());
        assertTrue(groupedTasks.containsKey("thread-1"));
        assertTrue(groupedTasks.containsKey("thread-2"));
        assertTrue(groupedTasks.containsKey("thread"));

        assertEquals(2, groupedTasks.get("thread-1").size());
        assertEquals(2, groupedTasks.get("thread-2").size());
        assertEquals(1, groupedTasks.get("thread").size());
    }

    @Test
    public void worstTasksOfSortsTasksCorrectly()
    {
        List<DebuggableTask.RunningDebuggableTask> tasks = newArrayList(
        createTask("thread-1", 1, 200),
        createTask("thread-2", 100, 200),
        createTask("thread-3", 200, 200)
        );

        Map.Entry<String, List<DebuggableTask.RunningDebuggableTask>> entry = StatusLogger.getTasksByStage(tasks).entrySet().iterator().next();
        List<DebuggableTask.RunningDebuggableTask> sortedTasks = StatusLogger.worstTasksOf(entry);

        assertEquals("thread-3", sortedTasks.get(2).threadId());
        assertEquals("thread-2", sortedTasks.get(1).threadId());
        assertEquals("thread-1", sortedTasks.get(0).threadId());
    }

    private DebuggableTask.RunningDebuggableTask createTask(String threadId, long creationTimeNanos, long startTimeNanos)
    {
        DebuggableTask.RunningDebuggableTask task = mock(DebuggableTask.RunningDebuggableTask.class);
        when(task.threadId()).thenReturn(threadId);
        when(task.creationTimeNanos()).thenReturn(creationTimeNanos);
        when(task.startTimeNanos()).thenReturn(startTimeNanos);
        return task;
    }
}
