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
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Interface for catalog context, providing access to catalog configuration and file I/O loaders.
 *
 * <p>This interface decouples catalog context from specific implementations, allowing for better
 * separation of concerns between Hadoop-aware and Hadoop-free environments.
 *
 * @since 0.10.0
 */
@Public
public interface ICatalogContext extends Serializable {

    /**
     * Returns the catalog options.
     *
     * @return catalog options
     */
    Options options();

    /**
     * Returns the preferred file I/O loader, if configured.
     *
     * @return preferred file I/O loader, or null if not configured
     */
    @Nullable
    FileIOLoader preferIO();

    /**
     * Returns the fallback file I/O loader, if configured.
     *
     * @return fallback file I/O loader, or null if not configured
     */
    @Nullable
    FileIOLoader fallbackIO();

    /**
     * Creates a copy of this context with different options.
     *
     * @param options new options
     * @return a new context instance with the specified options
     */
    ICatalogContext copy(Options options);
}
