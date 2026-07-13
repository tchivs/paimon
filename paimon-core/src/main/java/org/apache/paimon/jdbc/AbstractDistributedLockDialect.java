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

import org.apache.paimon.options.Options;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Jdbc distributed lock interface. */
public abstract class AbstractDistributedLockDialect implements JdbcDistributedLockDialect {

    private static final int OWNER_ID_MAX_LENGTH = 64;

    @Override
    public void createTable(JdbcClientPool connections, Options options)
            throws SQLException, InterruptedException {
        Integer lockKeyMaxLength = JdbcCatalogOptions.lockKeyMaxLength(options);
        connections.run(
                conn -> {
                    try (ResultSet tableExists =
                            conn.getMetaData()
                                    .getTables(
                                            null,
                                            null,
                                            JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME,
                                            null)) {
                        if (tableExists.next()) {
                            ensureOwnerColumn(conn);
                            return true;
                        }
                    }
                    String createDistributedLockTableSql =
                            String.format(getCreateTableSql(), lockKeyMaxLength);
                    try (PreparedStatement statement =
                            conn.prepareStatement(createDistributedLockTableSql)) {
                        statement.execute();
                        return true;
                    } catch (SQLException e) {
                        // Another catalog process may create the table after the metadata check.
                        // Re-check on the same connection before surfacing a real DDL failure.
                        try (ResultSet tableExists =
                                conn.getMetaData()
                                        .getTables(
                                                null,
                                                null,
                                                JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME,
                                                null)) {
                            if (tableExists.next()) {
                                ensureOwnerColumn(conn);
                                return true;
                            }
                        }
                        throw e;
                    }
                });
    }

    private void ensureOwnerColumn(java.sql.Connection connection) throws SQLException {
        try (ResultSet columns =
                connection
                        .getMetaData()
                        .getColumns(
                                null,
                                null,
                                JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME,
                                JdbcUtils.OWNER_ID)) {
            if (columns.next()) {
                return;
            }
        }

        try (PreparedStatement statement = connection.prepareStatement(getAddOwnerColumnSql())) {
            statement.execute();
        } catch (SQLException e) {
            // Another catalog process may migrate an existing table concurrently.
            try (ResultSet columns =
                    connection
                            .getMetaData()
                            .getColumns(
                                    null,
                                    null,
                                    JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME,
                                    JdbcUtils.OWNER_ID)) {
                if (columns.next()) {
                    return;
                }
            }
            throw e;
        }
    }

    protected String getAddOwnerColumnSql() {
        return "ALTER TABLE "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " ADD COLUMN "
                + JdbcUtils.OWNER_ID
                + " VARCHAR("
                + OWNER_ID_MAX_LENGTH
                + ")";
    }

    public abstract String getCreateTableSql();

    @Override
    public boolean lockAcquire(
            JdbcClientPool connections, String lockId, String ownerId, long timeoutMillSeconds)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getLockAcquireSql())) {
                        preparedStatement.setString(1, lockId);
                        preparedStatement.setString(2, ownerId);
                        preparedStatement.setLong(3, timeoutMillSeconds / 1000);
                        return preparedStatement.executeUpdate() > 0;
                    } catch (SQLException ex) {
                        return false;
                    }
                });
    }

    public abstract String getLockAcquireSql();

    @Override
    public boolean releaseLock(JdbcClientPool connections, String lockId, String ownerId)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getReleaseLockSql())) {
                        preparedStatement.setString(1, lockId);
                        preparedStatement.setString(2, ownerId);
                        return preparedStatement.executeUpdate() > 0;
                    }
                });
    }

    public abstract String getReleaseLockSql();

    @Override
    public int tryReleaseTimedOutLock(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getTryReleaseTimedOutLock())) {
                        preparedStatement.setString(1, lockId);
                        return preparedStatement.executeUpdate();
                    }
                });
    }

    public abstract String getTryReleaseTimedOutLock();
}
