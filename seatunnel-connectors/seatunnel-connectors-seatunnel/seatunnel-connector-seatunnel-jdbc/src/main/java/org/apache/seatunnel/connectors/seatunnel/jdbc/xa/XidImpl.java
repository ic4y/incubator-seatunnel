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



import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;


/**
 * A simple {@link Xid} implementation that stores branch and global transaction identifiers as byte
 * arrays.
 */
// TODO 测试需要才添加的public
public final class XidImpl implements Xid, Serializable {

    private static final long serialVersionUID = 1L;
    private static final char[] HEX_CHARS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    private final int formatId;
    @Nonnull private final byte[] globalTransactionId;
    @Nonnull private final byte[] branchQualifier;

    // TODO 测试需要才添加的public
    public XidImpl(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
        Preconditions.checkArgument(globalTransactionId.length <= Xid.MAXGTRIDSIZE);
        Preconditions.checkArgument(branchQualifier.length <= Xid.MAXBQUALSIZE);
        this.formatId = formatId;
        this.globalTransactionId = Arrays.copyOf(globalTransactionId, globalTransactionId.length);
        this.branchQualifier = Arrays.copyOf(branchQualifier, branchQualifier.length);
    }

    @Override
    public int getFormatId() {
        return formatId;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    @Override
    public byte[] getBranchQualifier() {
        return branchQualifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof XidImpl)) {
            return false;
        }
        XidImpl xid = (XidImpl) o;
        return formatId == xid.formatId
                && Arrays.equals(globalTransactionId, xid.globalTransactionId)
                && Arrays.equals(branchQualifier, xid.branchQualifier);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(formatId);
        result = 31 * result + Arrays.hashCode(globalTransactionId);
        result = 31 * result + Arrays.hashCode(branchQualifier);
        return result;
    }

    @Override
    public String toString() {
        return formatId
                + ":"
                + byteToHexString(globalTransactionId)
                + ":"
                + byteToHexString(branchQualifier);
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @param start start index, inclusively
     * @param end end index, exclusively
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes, final int start, final int end) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes == null");
        }

        int length = end - start;
        char[] out = new char[length * 2];

        for (int i = start, j = 0; i < end; i++) {
            out[j++] = HEX_CHARS[(0xF0 & bytes[i]) >>> 4];
            out[j++] = HEX_CHARS[0x0F & bytes[i]];
        }

        return new String(out);
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }
}
