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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class JdbcSinkWriter implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> {

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;
    private final SinkWriter.Context context;

    public JdbcSinkWriter(
        SinkWriter.Context context,
        JdbcStatementBuilder<SeaTunnelRow> statementBuilder,
        JdbcConnectorOptions jdbcConnectorOptions)
        throws IOException {

        JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcConnectorOptions);

        this.context = context;
        this.outputFormat = new JdbcOutputFormat<>(
            connectionProvider,
            jdbcConnectorOptions,
            () -> JdbcBatchStatementExecutor.simple(jdbcConnectorOptions.getQuery(), statementBuilder, Function.identity()));
        outputFormat.open();
    }

    @Override
    public List<JdbcSinkState> snapshotState() {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element)
        throws IOException {
        outputFormat.writeRecord(element);
    }

    @Override
    public Optional<XidInfo> prepareCommit()
        throws IOException {
        outputFormat.flush();
        return Optional.empty();
    }

    @Override
    public void abort() {

    }

    @Override
    public void close()
        throws IOException {
        outputFormat.close();
    }
}
