/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commiter.IcebergAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commiter.IcebergCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commiter.IcebregSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.IcebergSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.SeaTunnelRowDataTaskWriterFactory;

import org.apache.iceberg.Table;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Slf4j
@AutoService(SeaTunnelSink.class)
public class IcebergSink
        implements SeaTunnelSink<
                SeaTunnelRow, IcebergSinkState, IcebergCommitInfo, IcebergAggregatedCommitInfo> {

    private SeaTunnelRowType seaTunnelRowType;

    private JobContext jobContext;

    private SinkConfig sinkConfig;

    private CatalogTable catalogTable;

    private SeaTunnelRowDataTaskWriterFactory seaTunnelRowDataTaskWriterFactory;

    private Table table;

    private List<String> equalityFieldColumns;

    public IcebergSink() {}

    @SneakyThrows
    public IcebergSink(CatalogTable catalogTable, ReadonlyConfig readonlyConfig) {
        this.sinkConfig = new SinkConfig(readonlyConfig);
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        if (null != catalogTable.getTableSchema().getPrimaryKey()) {
            this.equalityFieldColumns =
                    catalogTable.getTableSchema().getPrimaryKey().getColumnNames();
        }
        try (IcebergTableLoader icebergTableLoader = IcebergTableLoader.create(sinkConfig)) {
            icebergTableLoader.open();
            this.table = icebergTableLoader.loadTable();
        }
        this.seaTunnelRowDataTaskWriterFactory =
                new SeaTunnelRowDataTaskWriterFactory(
                        IcebergTableLoader.create(sinkConfig),
                        seaTunnelRowType,
                        sinkConfig.getTargetFileSizeBytes(),
                        sinkConfig.getFileFormat(),
                        new HashMap<>(),
                        checkAndGetEqualityFieldIds(),
                        true);
    }

    @Override
    public String getPluginName() {
        return "Iceberg";
    }

    @Override
    @SneakyThrows
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.sinkConfig = new SinkConfig(ReadonlyConfig.fromConfig(pluginConfig));
        try (IcebergTableLoader icebergTableLoader = IcebergTableLoader.create(sinkConfig)) {
            icebergTableLoader.open();
            this.table = icebergTableLoader.loadTable();
        }
        if (null == seaTunnelRowDataTaskWriterFactory) {
            seaTunnelRowDataTaskWriterFactory =
                    new SeaTunnelRowDataTaskWriterFactory(
                            IcebergTableLoader.create(sinkConfig),
                            seaTunnelRowType,
                            sinkConfig.getTargetFileSizeBytes(),
                            sinkConfig.getFileFormat(),
                            new HashMap<>(),
                            checkAndGetEqualityFieldIds(),
                            true);
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        if (null == this.seaTunnelRowType) {
            this.seaTunnelRowType = seaTunnelRowType;
            this.equalityFieldColumns = sinkConfig.getPrimaryKeys();
        }

        if (null == seaTunnelRowDataTaskWriterFactory) {
            seaTunnelRowDataTaskWriterFactory =
                    new SeaTunnelRowDataTaskWriterFactory(
                            IcebergTableLoader.create(sinkConfig),
                            seaTunnelRowType,
                            sinkConfig.getTargetFileSizeBytes(),
                            sinkConfig.getFileFormat(),
                            new HashMap<>(),
                            checkAndGetEqualityFieldIds(),
                            true);
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, IcebergCommitInfo, IcebergSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new IcebergSinkWriter(seaTunnelRowDataTaskWriterFactory, context);
    }

    @Override
    public Optional<SinkAggregatedCommitter<IcebergCommitInfo, IcebergAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(
                new IcebregSinkAggregatedCommitter(
                        new HashMap<>(), IcebergTableLoader.create(sinkConfig)));
    }

    @Override
    public Optional<Serializer<IcebergAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    private List<Integer> checkAndGetEqualityFieldIds() {
        List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
        if (equalityFieldColumns != null && equalityFieldColumns.size() > 0) {
            Set<Integer> equalityFieldSet =
                    Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
            for (String column : equalityFieldColumns) {
                org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
                checkNotNull(
                        field,
                        "Missing required equality field column '%s' in table schema %s",
                        column,
                        table.schema());
                equalityFieldSet.add(field.fieldId());
            }

            if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
                log.warn(
                        "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
                                + " {}, use job specified equality field columns as the equality fields by default.",
                        equalityFieldSet,
                        table.schema().identifierFieldIds());
            }
            equalityFieldIds = Lists.newArrayList(equalityFieldSet);
        }
        return equalityFieldIds;
    }
}
