package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import com.google.common.base.Preconditions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExactlyOnceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExecutionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.SerializableSupplier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.XidGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:16
 */
public class JdbcExactlyOnceSinkWriter
        implements SinkWriter<SeaTunnelRow, JdbcCommitInfo, JdbcSinkState>
{
    private static final Logger LOG = LoggerFactory.getLogger(JdbcExactlyOnceSinkWriter.class);

    private final SinkWriter.Context context;
    private final XidGenerator xidGenerator;
//    private final XaFacade xaFacade;
    private final JdbcBatchStatementExecutor<SeaTunnelRow> jdbcBatchStatementExecutor;

    private transient Xid currentXid;

    public JdbcExactlyOnceSinkWriter(
            SinkWriter.Context context,
            String sql,
            JdbcStatementBuilder<SeaTunnelRow> statementBuilder,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            SerializableSupplier<XADataSource> dataSourceSupplier)
            throws SQLException, ClassNotFoundException
    {

        this.context = context;
        this.xidGenerator = XidGenerator.semanticXidGenerator();
        this.jdbcBatchStatementExecutor = JdbcBatchStatementExecutor.simple(sql, statementBuilder, Function.identity());
//        jdbcBatchStatementExecutor.prepareStatements(xaFacade.getOrEstablishConnection());

        //TODO 考虑是否需要创建线程定时执行executeBatch写入数据库，还是通过引擎调用prepareCommit写入数据库
    }

    public void open() {

    }

    @Override
    public List<JdbcSinkState> snapshotState()
            throws IOException
    {
        return Collections.singletonList(new JdbcSinkState(currentXid));
    }

    @Override
    public void write(SeaTunnelRow element)
            throws IOException
    {
        try {
            jdbcBatchStatementExecutor.addToBatch(element);
            jdbcBatchStatementExecutor.executeBatch();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //在执行这个方法后会执行snapshotState()方法
    @Override
    public Optional<JdbcCommitInfo> prepareCommit()
            throws IOException
    {
        try {
            jdbcBatchStatementExecutor.executeBatch();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public void abort()
    {

    }

    @Override
    public void close()
            throws IOException
    {
        try {
            jdbcBatchStatementExecutor.closeStatements();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建新的Xid
     * @param checkpointId
     * @throws Exception
     */
    private void beginTx(long checkpointId) throws Exception {
        Preconditions.checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(context, System.currentTimeMillis());
//        xaFacade.start(currentXid);

        // associate outputFormat with a new connection that might have been opened in start()

    }

    private Xid prepareCurrentTx() throws IOException {
        Preconditions.checkState(currentXid != null, "no current xid");

        //TODO 需要抽象outPutFormat 实现重试，自动提交等功能，两种SinkWriter共用
        try {
            jdbcBatchStatementExecutor.executeBatch();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        try {
//            xaFacade.endAndPrepare(currentXid);
        } catch (XaFacade.EmptyXaTransactionException e) {
            LOG.info(
                    "empty XA transaction (skip), xid: {}",
                    currentXid);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        currentXid = null;


        return null;
    }

}
