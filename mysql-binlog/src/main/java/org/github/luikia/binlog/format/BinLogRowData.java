package org.github.luikia.binlog.format;

import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class BinLogRowData implements Serializable {

    public static final TypeInformation<BinLogRowData> TYPE = Types.GENERIC(BinLogRowData.class);

    private static final long serialVersionUID = 1L;

    private String database;

    private String table;

    private String type;

    private long timestamp;

    private List<RowData> rows;

    @Data
    public static class RowData implements Serializable {

        private Map<String, Serializable> data;

        private Map<String, Serializable> old;
    }


}
