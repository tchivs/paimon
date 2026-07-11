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

package org.apache.paimon.catalog;

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;

import javax.annotation.Nullable;

import java.util.List;

/** Interface to commit snapshot atomically. */
public interface SnapshotCommit extends AutoCloseable {

    boolean commit(Snapshot snapshot, String branch, List<PartitionStatistics> statistics)
            throws Exception;

    /**
     * Commit a snapshot after validating it under the implementation's atomic commit boundary.
     * Implementations which cannot provide that boundary must reject a non-null validator.
     */
    default boolean commit(
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics,
            @Nullable CommitValidator validator)
            throws Exception {
        if (validator != null) {
            throw new UnsupportedOperationException(
                    "This snapshot commit implementation does not support atomic commit validation");
        }
        return commit(snapshot, branch, statistics);
    }
}
