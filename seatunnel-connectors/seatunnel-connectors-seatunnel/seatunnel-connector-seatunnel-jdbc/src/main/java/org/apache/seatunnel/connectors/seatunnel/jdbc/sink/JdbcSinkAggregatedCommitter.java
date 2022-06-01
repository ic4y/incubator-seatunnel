package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcCommitInfo;

import java.io.IOException;
import java.util.List;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:21
 */
public class JdbcSinkAggregatedCommitter implements SinkAggregatedCommitter<JdbcCommitInfo, JdbcAggregatedCommitInfo>
{
    @Override
    public List<JdbcAggregatedCommitInfo> commit(List<JdbcAggregatedCommitInfo> aggregatedCommitInfo)
            throws IOException
    {
        return null;
    }

    @Override
    public JdbcAggregatedCommitInfo combine(List<JdbcCommitInfo> commitInfos)
    {
        return null;
    }

    @Override
    public void abort(List<JdbcAggregatedCommitInfo> aggregatedCommitInfo)
            throws Exception
    {

    }

    @Override
    public void close()
            throws IOException
    {

    }
}
