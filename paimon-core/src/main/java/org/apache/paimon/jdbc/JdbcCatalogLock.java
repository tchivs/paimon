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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.utils.TimeUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.options.CatalogOptions.LOCK_ACQUIRE_TIMEOUT;
import static org.apache.paimon.options.CatalogOptions.LOCK_CHECK_MAX_SLEEP;

/** Jdbc catalog lock. */
public class JdbcCatalogLock implements CatalogLock {
    private final JdbcClientPool connections;
    private final long checkMaxSleep;
    private final long acquireTimeout;
    private final String catalogKey;
    private final boolean ownsConnections;

    public JdbcCatalogLock(
            JdbcClientPool connections,
            String catalogKey,
            long checkMaxSleep,
            long acquireTimeout) {
        this(connections, catalogKey, checkMaxSleep, acquireTimeout, false);
    }

    public JdbcCatalogLock(
            JdbcClientPool connections,
            String catalogKey,
            long checkMaxSleep,
            long acquireTimeout,
            boolean ownsConnections) {
        this.connections = connections;
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
        this.catalogKey = catalogKey;
        this.ownsConnections = ownsConnections;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        String lockUniqueName = String.format("%s.%s.%s", catalogKey, database, table);
        String ownerId = UUID.randomUUID().toString();
        lock(lockUniqueName, ownerId);
        AtomicReference<Exception> renewalFailure = new AtomicReference<>();
        ScheduledExecutorService heartbeatExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "paimon-jdbc-lock-heartbeat");
                            thread.setDaemon(true);
                            return thread;
                        });
        long heartbeatIntervalMillis = Math.max(100, acquireTimeout / 3);
        ScheduledFuture<?> heartbeat =
                heartbeatExecutor.scheduleWithFixedDelay(
                        () -> renew(lockUniqueName, ownerId, renewalFailure),
                        heartbeatIntervalMillis,
                        heartbeatIntervalMillis,
                        TimeUnit.MILLISECONDS);
        try {
            T result = callable.call();
            Exception failure = renewalFailure.get();
            if (failure != null) {
                throw failure;
            }
            return result;
        } finally {
            heartbeat.cancel(true);
            heartbeatExecutor.shutdownNow();
            JdbcUtils.release(connections, lockUniqueName, ownerId);
        }
    }

    private void renew(
            String lockUniqueName, String ownerId, AtomicReference<Exception> renewalFailure) {
        if (renewalFailure.get() != null) {
            return;
        }
        try {
            if (!JdbcUtils.renew(connections, lockUniqueName, ownerId)) {
                renewalFailure.compareAndSet(
                        null,
                        new IllegalStateException(
                                "Lost JDBC catalog lock ownership while renewing "
                                        + lockUniqueName));
            }
        } catch (Exception e) {
            renewalFailure.compareAndSet(
                    null,
                    new RuntimeException("Failed to renew JDBC catalog lock " + lockUniqueName, e));
        }
    }

    private void lock(String lockUniqueName, String ownerId)
            throws SQLException, InterruptedException {
        boolean lock = JdbcUtils.acquire(connections, lockUniqueName, ownerId, acquireTimeout);
        long nextSleep = 50;
        long startRetry = System.currentTimeMillis();
        while (!lock) {
            nextSleep *= 2;
            if (nextSleep > checkMaxSleep) {
                nextSleep = checkMaxSleep;
            }
            Thread.sleep(nextSleep);
            lock = JdbcUtils.acquire(connections, lockUniqueName, ownerId, acquireTimeout);
            if (System.currentTimeMillis() - startRetry > acquireTimeout) {
                break;
            }
        }
        long retryDuration = System.currentTimeMillis() - startRetry;
        if (!lock) {
            throw new RuntimeException(
                    "Acquire lock failed with time: " + Duration.ofMillis(retryDuration));
        }
    }

    @Override
    public void close() throws IOException {
        if (ownsConnections) {
            connections.close();
        }
    }

    public static long checkMaxSleep(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_CHECK_MAX_SLEEP.key(),
                                TimeUtils.getStringInMillis(LOCK_CHECK_MAX_SLEEP.defaultValue())))
                .toMillis();
    }

    public static long acquireTimeout(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_ACQUIRE_TIMEOUT.key(),
                                TimeUtils.getStringInMillis(LOCK_ACQUIRE_TIMEOUT.defaultValue())))
                .toMillis();
    }
}
