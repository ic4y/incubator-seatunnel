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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.JdbcExactlyOnceSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.JdbcSinkCommitter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class JdbcSink implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo> {

    private Config pluginConfig;

    private SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;

    private SeaTunnelContext seaTunnelContext;

    private JdbcConnectorOptions jdbcConnectorOptions;

    @Override
    public String getPluginName() {
        return "jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        this.jdbcConnectorOptions = new JdbcConnectorOptions(this.pluginConfig);
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(SinkWriter.Context context)
        throws IOException {
        SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> sinkWriter;
        // TODO SeatunnelTyoeInfo is not good enough to get typesArray
        JdbcStatementBuilder<SeaTunnelRow> statementBuilder = (st, row) -> JdbcUtils.setRecordToStatement(st, null, row);
        if (jdbcConnectorOptions.isExactlyOnce()) {
            sinkWriter = new JdbcExactlyOnceSinkWriter(
                context,
                statementBuilder,
                jdbcConnectorOptions
            );
        }
        else {
            sinkWriter = new JdbcSinkWriter(
                context,
                statementBuilder,
                jdbcConnectorOptions);
        }

        return sinkWriter;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return seaTunnelContext;
    }

    @Override
    public Optional<SinkCommitter<XidInfo>> createCommitter()
        throws IOException {
        if (jdbcConnectorOptions.isExactlyOnce()) {
            return Optional.of(new JdbcSinkCommitter(jdbcConnectorOptions));
        }
        return Optional.empty();
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
}
