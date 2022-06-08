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

import org.apache.seatunnel.api.sink.SinkWriter;

import javax.transaction.xa.Xid;

import java.security.SecureRandom;

/**
 * Generates {@link Xid} from:
 *
 * <ol>
 *   <li>To provide uniqueness over other jobs and apps, and other instances
 *   <li>of this job, gtrid consists of
 *   <li>subtask index (4 bytes)
 *   <li>currentTimeMillis id (4 bytes)
 *   <li>bqual consists of 4 random bytes (generated using {@link SecureRandom})
 * </ol>
 *
 * <p>Each {@link SemanticXidGenerator} instance MUST be used for only one Sink (otherwise Xids will
 * collide).
 */
class SemanticXidGenerator
        implements XidGenerator {

    private static final long serialVersionUID = 1L;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final int FORMAT_ID = 201;

    private transient byte[] gtridBuffer;
    private transient byte[] bqualBuffer;

    @Override
    public void open() {
        // globalTransactionId = task index + currentTimeMs
        gtridBuffer = new byte[Integer.BYTES + Long.BYTES];
        // branchQualifier = random bytes
        bqualBuffer = getRandomBytes(Integer.BYTES);
    }

    @Override
    public Xid generateXid(SinkWriter.Context context, long currentTimeMillis) {

        writeNumber(context.getIndexOfSubtask(), Integer.BYTES, gtridBuffer, 0);
        writeNumber(currentTimeMillis, Long.BYTES, gtridBuffer, Integer.BYTES);
        // relying on arrays copying inside XidImpl constructor
        return new XidImpl(FORMAT_ID, gtridBuffer, bqualBuffer);
    }

    @Override
    public boolean belongsToSubtask(Xid xid, SinkWriter.Context  ctx) {
        if (xid.getFormatId() != FORMAT_ID) {
            return false;
        }
        int subtaskIndex = readNumber(xid.getGlobalTransactionId(), 0, Integer.BYTES);
        return subtaskIndex == ctx.getIndexOfSubtask()
                || subtaskIndex > ctx.getNumberOfParallelSubtasks() - 1;
    }

    private static int readNumber(byte[] bytes, int offset, int numBytes) {
        final int number = 0xff;
        int result = 0;
        for (int i = 0; i < numBytes; i++) {
            result |= (bytes[offset + i] & number) << Byte.SIZE * i;
        }
        return result;
    }

    private static void writeNumber(long number, int numBytes, byte[] dst, int dstOffset) {
        for (int i = dstOffset; i < dstOffset + numBytes; i++) {
            dst[i] = (byte) number;
            number >>>= Byte.SIZE;
        }
    }

    private byte[] getRandomBytes(int size) {
        byte[] bytes = new byte[size];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }
}
