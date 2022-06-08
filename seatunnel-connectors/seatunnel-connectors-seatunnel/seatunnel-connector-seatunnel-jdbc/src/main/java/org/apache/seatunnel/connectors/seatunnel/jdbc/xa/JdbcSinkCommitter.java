package org.apache.seatunnel.connectors.seatunnel.jdbc.xa;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import java.io.IOException;
import java.util.List;

public class JdbcSinkCommitter
    implements SinkCommitter<XidInfo> {
    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final JdbcConnectorOptions jdbcConnectorOptions;

    public JdbcSinkCommitter(
        JdbcConnectorOptions jdbcConnectorOptions
    )
        throws IOException {
        this.jdbcConnectorOptions = jdbcConnectorOptions;
        this.xaFacade = XaFacade.fromJdbcConnectionOptions(
            jdbcConnectorOptions);
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
        try {
            xaFacade.open();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<XidInfo> commit(List<XidInfo> committables) {
        return xaGroupOps
            .commit(committables, false, jdbcConnectorOptions.getMaxCommitAttempts())
            .getForRetry();
    }

    @Override
    public void abort(List<XidInfo> commitInfos)
        throws IOException {
        try {
            xaGroupOps.rollback(commitInfos);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }
}
