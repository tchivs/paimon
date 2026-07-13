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

import org.apache.paimon.annotation.Public;

/**
 * Capability of a catalog whose snapshot commit is an atomic compare-and-set.
 *
 * <p>The catalog must reject a snapshot when a newer snapshot is already visible and return {@code
 * false}. This allows a client-side commit validator to run against the observed latest snapshot
 * while the catalog CAS closes the race before publication.
 */
@Public
public interface AtomicSnapshotCommitCatalog {
    boolean supportsAtomicSnapshotCommit();
}
