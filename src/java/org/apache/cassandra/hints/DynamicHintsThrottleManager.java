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
package org.apache.cassandra.hints;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.HintsServiceMetrics;

/**
 * Manages dynamic throttling of hints delivery based on throughput metrics and backlog size.
 *
 * The manager adjusts the rate limiter based on the following strategy:
 * - High backlog + throughput close to limit → increase throttle (we're bottlenecked and can deliver faster)
 * - High backlog + throughput much less than limit → keep throttle (hints are slow/failing, increasing won't help)
 * - Low backlog → decrease throttle to base rate (no urgency)
 */
final class DynamicHintsThrottleManager
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicHintsThrottleManager.class);

    private static final double THROUGHPUT_UTILIZATION_THRESHOLD = getThroughputUtilizationThreshold();

    private static double getThroughputUtilizationThreshold()
    {
        String property = System.getProperty("cassandra.hints.throttle.throughput_utilization_threshold");
        if (property != null)
        {
            try
            {
                double value = Double.parseDouble(property);
                if (value > 0.0 && value <= 1.0)
                {
                    logger.info("Using throughput utilization threshold {} from system property", value);
                    return value;
                }
                else
                {
                    logger.warn("Invalid throughput utilization threshold {} from system property, must be between 0.0 and 1.0, using default 0.8", value);
                }
            }
            catch (NumberFormatException e)
            {
                logger.warn("Invalid throughput utilization threshold '{}' from system property, using default 0.8", property);
            }
        }
        return 0.8;
    }

    private static final double INCREASE_FACTOR = 1.1;
    private static final double DECREASE_FACTOR = 0.9;

    private final HintsCatalog catalog;
    private final AtomicReference<RateLimiter> rateLimiterRef;

    DynamicHintsThrottleManager(HintsCatalog catalog, AtomicReference<RateLimiter> rateLimiterRef)
    {
        this.catalog = catalog;
        this.rateLimiterRef = rateLimiterRef;
    }

    /**
     * Updates the throttle rate based on current throughput and backlog.
     * This method is called periodically by the HintsService.
     */
    void updateThrottle()
    {
        int backlogThreshold = DatabaseDescriptor.getHintedHandoffThrottleBacklogThreshold();
        int maxThrottleKiB = DatabaseDescriptor.getHintedHandoffMaxThrottleInKiB();
        int baseThrottleKiB = DatabaseDescriptor.getHintedHandoffThrottleInKiB();

        long backlogCount = catalog.stores()
                                   .mapToInt(HintsStore::getDispatchQueueSize)
                                   .sum();

        double currentThroughputBytesPerSec = HintsServiceMetrics.hintsThroughputBytes.getOneMinuteRate();

        RateLimiter rateLimiter = rateLimiterRef.get();
        if (rateLimiter == null)
        {
            logger.warn("Rate limiter is null, skipping throttle update");
            return;
        }

        double currentRateBytesPerSec = rateLimiter.getRate();
        if (currentRateBytesPerSec == 0)
        {
            return;
        }

        double baseRateBytesPerSec = baseThrottleKiB * 1024.0;
        double maxRateBytesPerSec = (maxThrottleKiB == 0) ? Double.MAX_VALUE : maxThrottleKiB * 1024.0;

        double newRate = currentRateBytesPerSec;

        if (backlogCount > backlogThreshold)
        {
            double utilizationRatio = currentThroughputBytesPerSec / currentRateBytesPerSec;

            if (utilizationRatio >= THROUGHPUT_UTILIZATION_THRESHOLD && currentRateBytesPerSec < maxRateBytesPerSec)
            {
                newRate = Math.min(currentRateBytesPerSec * INCREASE_FACTOR, maxRateBytesPerSec);
                logger.info("Increasing hints throttle from {} to {} bytes/sec (backlog: {}, throughput: {} bytes/sec)",
                           currentRateBytesPerSec, newRate, backlogCount, currentThroughputBytesPerSec);
            }
            else
            {
                logger.debug("Hints backlog is {} but throughput ({} bytes/sec) is only {}% of rate ({} bytes/sec) - not increasing throttle",
                            backlogCount, currentThroughputBytesPerSec, (int)(utilizationRatio * 100), currentRateBytesPerSec);
            }
        }
        else
        {
            if (currentRateBytesPerSec > baseRateBytesPerSec)
            {
                newRate = Math.max(currentRateBytesPerSec * DECREASE_FACTOR, baseRateBytesPerSec);
                logger.info("Decreasing hints throttle from {} to {} bytes/sec (backlog: {} is below threshold: {})",
                           currentRateBytesPerSec, newRate, backlogCount, backlogThreshold);
            }
        }

        if (Math.abs(newRate - currentRateBytesPerSec) > 1.0) // Only update if change is significant
        {
            rateLimiter.setRate(newRate);
        }
    }
}