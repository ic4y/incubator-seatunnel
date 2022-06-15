package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Optional;

@Data
@AllArgsConstructor
public class JdbcSourceSplit implements SourceSplit {

    private final Optional<Object[]> parameterValueOptional;

    @Override
    public String splitId() {
        return null;
    }
}
