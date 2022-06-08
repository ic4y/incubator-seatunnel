package org.apache.seatunnel.connectors.seatunnel.jdbc.xa;

import com.google.common.base.Preconditions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExactlyOnceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExecutionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:16
 */
public class JdbcExactlyOnceSinkWriter
        implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState>
{
    private static final Logger LOG = LoggerFactory.getLogger(JdbcExactlyOnceSinkWriter.class);

    private final SinkWriter.Context context;

    private final XaFacade xaFacade;

    private final XidGenerator xidGenerator;

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;


    private transient Xid currentXid;

    public JdbcExactlyOnceSinkWriter(
            SinkWriter.Context context,
            JdbcConnectorOptions jdbcConnectorOptions)
            throws IOException
    {
        Preconditions.checkArgument(
                jdbcConnectorOptions.getMaxRetries() == 0,
                "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                        + "cause duplicates. See issue FLINK-22311 for details.");

        this.context = context;
        this.xidGenerator = XidGenerator.semanticXidGenerator();
        this.xaFacade =  XaFacade.fromJdbcConnectionOptions(
                jdbcConnectorOptions);

        JdbcStatementBuilder<SeaTunnelRow> statementBuilder = (st, row) -> JdbcUtils.setRecordToStatement(st, null, row);

        this.outputFormat = new JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>(
                xaFacade,
                        jdbcConnectorOptions,
                        () -> JdbcBatchStatementExecutor.simple(
                                jdbcConnectorOptions.getQuery(), statementBuilder, Function.identity()));

        open();
    }

    public void open() throws IOException{
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
    public List<JdbcSinkState> snapshotState()
            throws IOException
    {
        return Collections.emptyList();
//        return Collections.singletonList(new JdbcSinkState(currentXid));
    }

    @Override
    public  void write(SeaTunnelRow element)
            throws IOException
    {
        System.out.println(Arrays.toString(element.getFields()));
        Preconditions.checkState(currentXid != null, "current xid must not be null");
        outputFormat.writeRecord(element);
    }

    //在执行这个方法后会执行snapshotState()方法
    @Override
    public Optional<XidInfo> prepareCommit()
            throws IOException
    {
        prepareCurrentTx();
        Optional<XidInfo> currentXid = Optional.of(new XidInfo(this.currentXid, 0));
        this.currentXid = null;
        beginTx();
        System.out.println("--prepareCommit------->"+currentXid.get().getXid());
        return currentXid;
    }

    @Override
    public void abort()
    {

    }

    @Override
    public void close()
            throws IOException
    {
        if (currentXid != null && xaFacade.isOpen()) {
            try {
                LOG.debug("remove current transaction before closing, xid={}", currentXid);
                xaFacade.failAndRollback(currentXid);
            } catch (Exception e) {
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
        // don't format.close(); as we don't want neither to flush nor to close connection here
        currentXid = null;

    }

    private void beginTx() throws IOException {
        Preconditions.checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(context, System.currentTimeMillis());
        try {
            xaFacade.start(currentXid);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void prepareCurrentTx() throws IOException {
        Preconditions.checkState(currentXid != null, "no current xid");
        outputFormat.flush();
        try {
            xaFacade.endAndPrepare(currentXid);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
    }

}
