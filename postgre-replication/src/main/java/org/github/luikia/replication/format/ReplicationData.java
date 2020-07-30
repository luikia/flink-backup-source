package org.github.luikia.replication.format;

import com.google.gson.*;
import lombok.Data;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.postgresql.replication.LogSequenceNumber;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@Data
public class ReplicationData implements Serializable {
    public static final TypeInformation<ReplicationData> TYPE = Types.GENERIC(ReplicationData.class);

    private static final long serialVersionUID = 1L;

    private static final Gson g = new Gson();

    private List<ReplicationChange> changes;

    private String lsn;

    public ReplicationData() {
    }

    public ReplicationData(LogSequenceNumber lsn, ByteBuffer msg) {
        this.lsn = lsn.asString();
        if (Objects.nonNull(msg)) {
            int offset = msg.arrayOffset();
            int length = msg.array().length - offset;
            String json = new String(msg.array(), offset, length);
            JsonObject jsonObject = g.fromJson(json, JsonObject.class);
            JsonArray changeArr = jsonObject.get("change").getAsJsonArray();
            changes = StreamSupport.stream(changeArr.spliterator(), false)
                    .map(ReplicationChange::new)
                    .collect(Collectors.toList());
        }

    }

    @Data
    public class ReplicationChange implements Serializable {

        private String type;

        private String schema;

        private String table;

        private Map<String, Serializable> data;

        private Map<String, Serializable> keys;

        private ReplicationChange(JsonElement j) {
            JsonObject json = j.getAsJsonObject();
            this.type = json.get("kind").getAsString();
            this.schema = json.get("schema").getAsString();
            this.table = json.get("table").getAsString();
            if (!"delete".equalsIgnoreCase(this.type)) {
                JsonArray columnnames = json.getAsJsonArray("columnnames");
                JsonArray columnValues = json.getAsJsonArray("columnvalues");
                this.data = new HashedMap(columnnames.size());
                IntStream.range(0, columnnames.size()).forEach(i ->
                        this.data.put(columnnames.get(i).getAsString(),
                                this.toSerializable(columnValues.get(i))));
            }
            if (!"insert".equalsIgnoreCase(this.type)) {
                JsonArray keynames = json.get("oldkeys").getAsJsonObject().get("keynames").getAsJsonArray();
                JsonArray keyvalues = json.get("oldkeys").getAsJsonObject().get("keyvalues").getAsJsonArray();
                keys = new HashedMap(keynames.size());
                IntStream.range(0, keynames.size()).forEach(i ->
                        this.keys.put(keynames.get(i).getAsString(),
                                this.toSerializable(keyvalues.get(i))));
            }

        }

        private Serializable toSerializable(JsonElement data) {
            if (data.isJsonPrimitive()) {
                JsonPrimitive jsonPrimtive = data.getAsJsonPrimitive();
                if (jsonPrimtive.isBoolean()) {
                    return jsonPrimtive.getAsBoolean();
                } else if (jsonPrimtive.isNumber()) {
                    return jsonPrimtive.getAsNumber();
                } else if (jsonPrimtive.isString()) {
                    return jsonPrimtive.getAsString();
                } else {
                    return null;
                }
            }
            return null;
        }
    }
}
