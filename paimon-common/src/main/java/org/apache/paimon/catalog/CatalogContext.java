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
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.Serial;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.HadoopUtils.HADOOP_LOAD_DEFAULT_CONFIG;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Context of catalog.
 *
 * @since 0.4.0
 */
@Public
public class CatalogContext implements ICatalogContext {

    @Serial private static final long serialVersionUID = 1L;

    protected final Options options;
    @Nullable protected final FileIOLoader preferIOLoader;
    @Nullable protected final FileIOLoader fallbackIOLoader;

    protected CatalogContext(
            Options options,
            @Nullable FileIOLoader preferIOLoader,
            @Nullable FileIOLoader fallbackIOLoader) {
        this.options = checkNotNull(options);
        this.preferIOLoader = preferIOLoader;
        this.fallbackIOLoader = fallbackIOLoader;
    }

    public CatalogContext copy(Options options) {
        return create(options, this.preferIOLoader, this.fallbackIOLoader);
    }

    public static CatalogContext create(Path warehouse) {
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toUri().toString());
        return create(options);
    }

    public static CatalogContext create(Options options) {
        return create(options, null, null);
    }

    public static CatalogContext create(Options options, FileIOLoader fallbackIOLoader) {
        return create(options, null, fallbackIOLoader);
    }

    public static CatalogContext create(
            Options options, FileIOLoader preferIOLoader, FileIOLoader fallbackIOLoader) {
        return shouldLoadHadoop(options)
                ? CatalogHadoopContext.create(options, preferIOLoader, fallbackIOLoader)
                : new CatalogContext(options, preferIOLoader, fallbackIOLoader);
    }

    private static boolean shouldLoadHadoop(Options options) {
        return options.getBoolean(HADOOP_LOAD_DEFAULT_CONFIG.key(), false);
    }

    public Options options() {
        return options;
    }

    @Nullable
    public FileIOLoader preferIO() {
        return preferIOLoader;
    }

    @Nullable
    public FileIOLoader fallbackIO() {
        return fallbackIOLoader;
    }
}
