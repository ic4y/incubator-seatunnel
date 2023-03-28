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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.catalog;

import oracle.jdbc.OracleType;

public class OracleDataTypeParser {

    public static OracleDataType parse(String type) {
        for (OracleType oracleType : OracleType.values()) {
            Integer precision = null;
            Integer scale = null;

            if (type.indexOf("(") != -1) {
                int left = type.indexOf("(");
                int right = type.indexOf(")");
                String[] precisionAndScale = type.substring(left + 1, right).split(",");
                if (precisionAndScale.length == 2) {
                    precision = Integer.parseInt(precisionAndScale[0]);
                    scale = Integer.parseInt(precisionAndScale[1]);
                } else if (precisionAndScale.length == 1) {
                    precision = Integer.parseInt(precisionAndScale[0]);
                } else {
                    throw new IllegalArgumentException("invalid type: " + type);
                }
                type = type.substring(0, left);
            }
            if (oracleType.name().equalsIgnoreCase(type)) {
                return new OracleDataType(type, oracleType, precision, scale);
            }
            if (oracleType.getName().equalsIgnoreCase(type)) {
                return new OracleDataType(type, oracleType, precision, scale);
            }
        }
        throw new IllegalArgumentException("invalid type: " + type);
    }
}
