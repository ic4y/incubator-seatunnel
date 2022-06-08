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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcSinkAggregatedCommitter implements SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo> {

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final JdbcConnectorOptions jdbcConnectorOptions;

    public JdbcSinkAggregatedCommitter(
        JdbcConnectorOptions jdbcConnectorOptions
    ) throws IOException {
        this.xaFacade = XaFacade.fromJdbcConnectionOptions(
            jdbcConnectorOptions);
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
        this.jdbcConnectorOptions = jdbcConnectorOptions;
        try {
            xaFacade.open();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<JdbcAggregatedCommitInfo> commit(List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) {
        List<JdbcAggregatedCommitInfo> collect = aggregatedCommitInfos.stream().map(aggregatedCommitInfo -> {
            XaGroupOps.GroupXaOperationResult<XidInfo> result = xaGroupOps.commit(aggregatedCommitInfo.getXidInfoList(), false, jdbcConnectorOptions.getMaxCommitAttempts());
            return new JdbcAggregatedCommitInfo(result.getForRetry());
        }).filter(ainfo -> !ainfo.getXidInfoList().isEmpty()).collect(Collectors.toList());
        return collect;
    }

    @Override
    public JdbcAggregatedCommitInfo combine(List<XidInfo> commitInfos) {
        return new JdbcAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<JdbcAggregatedCommitInfo> aggregatedCommitInfo) {
        for (JdbcAggregatedCommitInfo commitInfos : aggregatedCommitInfo) {
            xaGroupOps.rollback(commitInfos.getXidInfoList());
        }
    }

    @Override
    public void close()
        throws IOException {
        try {
            xaFacade.close();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }
}
