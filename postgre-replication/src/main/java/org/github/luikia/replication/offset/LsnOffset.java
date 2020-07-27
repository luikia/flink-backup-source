package org.github.luikia.replication.offset;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.github.luikia.offset.Offset;
import org.postgresql.replication.LogSequenceNumber;

public class LsnOffset implements Offset {

    private static final Gson g = new Gson();

    private String lsn;

    public LsnOffset() {
    }

    private LsnOffset(String lsn) {
        this.lsn = lsn;
    }

    public String getLsn() {
        return lsn;
    }

    public void setLsn(String lsn) {
        this.lsn = lsn;
    }

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
