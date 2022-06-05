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

package org.apache.seatunnel.api.sink;

import java.io.Serializable;
import java.util.Map;

/**
 * The default {@link SinkWriter.Context} implement class.
 */
public class DefaultSinkWriterContext implements SinkWriter.Context , Serializable
{

    private final Map<String, String> configuration;
    private final int subtask;
    private final int parallelism;

    public DefaultSinkWriterContext(Map<String, String> configuration, int subtask, int parallelism) {
        this.configuration = configuration;
        this.subtask = subtask;
        this.parallelism = parallelism;
    }

    @Override
    public Map<String, String> getConfiguration() {
        return configuration;
    }

    @Override
    public int getIndexOfSubtask() {
        return subtask;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return parallelism;
    }
}
