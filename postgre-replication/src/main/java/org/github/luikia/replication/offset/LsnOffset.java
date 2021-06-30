package org.github.luikia.replication.offset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.github.luikia.offset.Offset;
import org.postgresql.replication.LogSequenceNumber;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class LsnOffset extends Offset {

    private static final long serialVersionUID = 1L;

    private String lsn;
    
    public LogSequenceNumber getLogSequenceNumber() {
        return LogSequenceNumber.valueOf(this.lsn);
    }

    @Override
    public String toJsonString() {
        return g.toJson(this);
    }

    public static LsnOffset of(String lsn) {
        return new LsnOffset(lsn);
    }

    public static LsnOffset fromJson(String json) {
        return StringUtils.isNoneEmpty(json) ? g.fromJson(json, LsnOffset.class) : null;
    }
}
