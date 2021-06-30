package org.github.luikia.oplog.format;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.github.luikia.type.LogType;

import java.io.Serializable;


@Data
@NoArgsConstructor
public class OplogData implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final TypeInformation<OplogData> TYPE = Types.POJO(OplogData.class, ImmutableMap.<String, TypeInformation<?>>builder()
            .put("offset", Types.LONG)
            .put("namespace", Types.STRING)
            .put("namespace", Types.STRING)
            .put("type", Types.ENUM(LogType.class))
            .put("data", Types.STRING)
            .put("id", Types.STRING)
            .put("document", Types.STRING).build()
    );

    private Long offset;

    private String namespace;

    private LogType type;

    private String data;

    private String id;

    private String document;

    public OplogData(Document document) {
        this.document = document.toJson();
        this.namespace = document.getString("ns");
        this.type = LogType.getLogTypePrefix(document.getString("op"));
        Document o = document.get("o", Document.class);
        this.id = o.getObjectId("_id").toHexString();
        this.offset = document.get("ts", BsonTimestamp.class).getValue();
        this.data = o.toJson();
    }

    public OplogData(String json) {
        this(Document.parse(json));
    }

    public Document getDocument() {
        return Document.parse(this.document);
    }

    @Override
    public String toString() {
        return document;
    }
}
