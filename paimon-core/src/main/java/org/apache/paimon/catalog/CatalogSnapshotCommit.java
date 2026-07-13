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
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.List;

/** A {@link SnapshotCommit} using {@link Catalog} to commit. */
public class CatalogSnapshotCommit implements SnapshotCommit {

    private final Catalog catalog;
    private final Identifier identifier;
    @Nullable private final String uuid;
    @Nullable private final SnapshotManager snapshotManager;

    public CatalogSnapshotCommit(Catalog catalog, Identifier identifier, @Nullable String uuid) {
        this(catalog, identifier, uuid, null);
    }

    public CatalogSnapshotCommit(
            Catalog catalog,
            Identifier identifier,
            @Nullable String uuid,
            SnapshotManager snapshotManager) {
        this.catalog = catalog;
        this.identifier = identifier;
        this.uuid = uuid;
        this.snapshotManager = snapshotManager;
    }

    @Override
    public boolean commit(Snapshot snapshot, String branch, List<PartitionStatistics> statistics)
            throws Exception {
        Identifier newIdentifier =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branch);
        return catalog.commitSnapshot(newIdentifier, uuid, snapshot, statistics);
    }

    @Override
    public boolean supportsAtomicCommitValidation() {
        if (snapshotManager == null || !(catalog instanceof AtomicSnapshotCommitCatalog)) {
            return false;
        }
        return ((AtomicSnapshotCommitCatalog) catalog).supportsAtomicSnapshotCommit();
    }

    @Override
    public boolean commit(
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics,
            @Nullable CommitValidator validator)
            throws Exception {
        if (validator == null) {
            return commit(snapshot, branch, statistics);
        }
        if (!supportsAtomicCommitValidation()) {
            throw new UnsupportedOperationException(
                    "This catalog does not provide atomic snapshot compare-and-set for validation");
        }

        // The REST commit API performs the compare-and-set at publication. A concurrent commit
        // makes this call return false, allowing FileStoreCommitImpl to rebuild the snapshot and
        // invoke the validator again against the new latest snapshot.
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        if (!validator.validate(latestSnapshot, snapshot)) {
            return false;
        }
        return commit(snapshot, branch, statistics);
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
