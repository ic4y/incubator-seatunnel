package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JdbcSourceSplit implements SourceSplit {

    private Integer parameterId = -1;

    @Override
    public String splitId() {
        return parameterId.toString();
    }
}
