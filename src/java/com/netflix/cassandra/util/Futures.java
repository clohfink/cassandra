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

package com.netflix.cassandra.util;

import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class Futures
{

    private Futures() {}

    // for checkstyles needs to be fully qualified class name
    public static <T> AsyncPromise<T> toPromise(java.util.concurrent.CompletableFuture<T> cf) {
        AsyncPromise<T> promise = new AsyncPromise<>();
        cf.whenComplete((result, error) -> {
            if (error != null) {
                promise.setFailure(error);
            } else {
                promise.setSuccess(result);
            }
        });
        return promise;
    }
}
