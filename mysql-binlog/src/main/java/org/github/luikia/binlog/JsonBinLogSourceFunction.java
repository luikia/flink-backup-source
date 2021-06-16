package org.github.luikia.binlog;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.github.luikia.binlog.format.BinLogRowData;

import java.util.Objects;

@Slf4j
@NoArgsConstructor
public class JsonBinLogSourceFunction extends BinlogBaseSourceFunction<String> {

    private static final long serialVersionUID = 1L;

    private static final Gson g = new Gson();

    public JsonBinLogSourceFunction(String host, int port, String username, String password) {
        super(host, port, username, password);
    }

    @Override
    public String format(BinLogRowData data) {
        JsonObject json = new JsonObject();
        json.addProperty("database", data.getDatabase());
        json.addProperty("table", data.getTable());
        json.addProperty("timestamp", data.getTimestamp());
        json.addProperty("type", data.getType().name());
        JsonArray rows = new JsonArray();
        data.forEach(r -> rows.add(this.toRowJson(r)));
        json.add("rows", rows);
        log.debug("convert binlog to json:{}", json);
        return json.toString();
    }

    private JsonObject toRowJson(BinLogRowData.RowData r) {
        JsonObject row = new JsonObject();
        row.add("data", g.toJsonTree(r.getData()));
        if (Objects.nonNull(r.getOld()))
            row.add("old", g.toJsonTree(r.getOld()));
        return row;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
