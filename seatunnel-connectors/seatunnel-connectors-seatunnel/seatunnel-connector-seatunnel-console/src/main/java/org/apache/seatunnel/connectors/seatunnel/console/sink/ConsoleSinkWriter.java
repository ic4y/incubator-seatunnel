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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.console.state.ConsoleState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ConsoleSinkWriter implements SinkWriter<SeaTunnelRow, ConsoleCommitInfo, ConsoleState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleSinkWriter.class);

    private final SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;

    public ConsoleSinkWriter(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    @SuppressWarnings("checkstyle:RegexpSingleline")
    public void write(SeaTunnelRow element) {
        System.out.println(Arrays.toString(element.getFields()));
    }

    @Override
    public List<ConsoleState> snapshotState()
            throws IOException
    {
        return Arrays.asList(new ConsoleState());
    }

    @Override
    public Optional<ConsoleCommitInfo> prepareCommit() {
        System.out.println("----prepareCommit------");
        return Optional.of(new ConsoleCommitInfo("send commit"));
    }

    @Override
    public void abort() {

    }

    @Override
    public void close() {

    }
}
