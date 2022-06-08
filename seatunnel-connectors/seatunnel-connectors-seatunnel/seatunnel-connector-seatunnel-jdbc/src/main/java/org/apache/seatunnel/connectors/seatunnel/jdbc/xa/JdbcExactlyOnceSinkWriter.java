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

package org.apache.seatunnel.connectors.seatunnel.jdbc.xa;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class JdbcExactlyOnceSinkWriter
    implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcExactlyOnceSinkWriter.class);

    private final SinkWriter.Context context;

    private final XaFacade xaFacade;

    private final XidGenerator xidGenerator;

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;

    private transient Xid currentXid;

    public JdbcExactlyOnceSinkWriter(
        SinkWriter.Context context,
        JdbcStatementBuilder<SeaTunnelRow> statementBuilder,
        JdbcConnectorOptions jdbcConnectorOptions)
        throws IOException {
        checkArgument(
            jdbcConnectorOptions.getMaxRetries() == 0,
            "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                + "cause duplicates.");

        this.context = context;
        this.xidGenerator = XidGenerator.semanticXidGenerator();
        this.xaFacade = XaFacade.fromJdbcConnectionOptions(
            jdbcConnectorOptions);

        this.outputFormat = new JdbcOutputFormat<>(
            xaFacade,
            jdbcConnectorOptions,
            () -> JdbcBatchStatementExecutor.simple(
                jdbcConnectorOptions.getQuery(), statementBuilder, Function.identity()));

        open();
    }

    public void open() throws IOException {
        try {
            xidGenerator.open();
            xaFacade.open();
            outputFormat.open();
            beginTx();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState() {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element)
        throws IOException {
        checkState(currentXid != null, "current xid must not be null");
        outputFormat.writeRecord(element);
    }

    @Override
    public Optional<XidInfo> prepareCommit()
        throws IOException {
        prepareCurrentTx();
        Optional<XidInfo> currentXid = Optional.of(new XidInfo(this.currentXid, 0));
        this.currentXid = null;
        beginTx();
        return currentXid;
    }

    @Override
    public void abort() {

    }

    @Override
    public void close()
        throws IOException {
        if (currentXid != null && xaFacade.isOpen()) {
            try {
                LOG.debug("remove current transaction before closing, xid={}", currentXid);
                xaFacade.failAndRollback(currentXid);
            }
            catch (Exception e) {
                LOG.warn("unable to fail/rollback current transaction, xid={}", currentXid, e);
            }
        }
        try {
            xaFacade.close();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        xidGenerator.close();
        currentXid = null;
    }

    private void beginTx() throws IOException {
        checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(context, System.currentTimeMillis());
        try {
            xaFacade.start(currentXid);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void prepareCurrentTx() throws IOException {
        checkState(currentXid != null, "no current xid");
        outputFormat.flush();
        try {
            xaFacade.endAndPrepare(currentXid);
        }
        catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
    }
}
