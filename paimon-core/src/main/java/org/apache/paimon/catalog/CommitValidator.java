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

import javax.annotation.Nullable;

/**
 * Validates a prepared snapshot while the catalog's snapshot commit lock is held.
 *
 * <p>Returning {@code false} asks the file-store committer to rebuild the prepared snapshot against
 * the latest snapshot. Throwing a {@link CommitValidationException} rejects the commit without
 * retrying fragments that may contain stale routing.
 */
@FunctionalInterface
public interface CommitValidator {

    boolean validate(@Nullable Snapshot latestSnapshot, Snapshot committingSnapshot)
            throws Exception;
}
