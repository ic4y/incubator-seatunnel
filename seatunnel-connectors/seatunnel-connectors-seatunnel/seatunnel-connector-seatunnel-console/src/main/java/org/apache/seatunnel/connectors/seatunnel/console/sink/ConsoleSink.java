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

package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.console.state.ConsoleState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class ConsoleSink implements SeaTunnelSink<SeaTunnelRow, ConsoleState, ConsoleCommitInfo, ConsoleAggregatedCommitInfo> {

    private Config pluginConfig;
    private SeaTunnelContext seaTunnelContext;
    private SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;

    @Override
    public void setTypeInfo(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    public SinkWriter<SeaTunnelRow, ConsoleCommitInfo, ConsoleState> createWriter(SinkWriter.Context context) {
        return new ConsoleSinkWriter(seaTunnelRowTypeInfo);
    }

    @Override
    public Optional<SinkCommitter<ConsoleCommitInfo>> createCommitter()
            throws IOException
    {
        return Optional.of(new ConsoleCommitter());
    }

    @Override
    public Optional<Serializer<ConsoleCommitInfo>> getCommitInfoSerializer()
    {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public SinkWriter<SeaTunnelRow, ConsoleCommitInfo, ConsoleState> restoreWriter(
        SinkWriter.Context context, List<ConsoleState> states) {
        return restoreWriter(context, states);
    }

    @Override
    public String getPluginName() {
        return "Console";
    }

    @Override
    public void prepare(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }
}
