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
@Deprecated
public class OplogData implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final TypeInformation<OplogData> TYPE = Types.POJO(OplogData.class, ImmutableMap.<String, TypeInformation<?>>builder()
            .put("offset", Types.LONG)
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
        Document o2 = null;
        if(this.type == LogType.UPDATE){
            o2 = document.get("o2",Document.class);
            this.data = o2.toJson();
            this.id = o2.getObjectId("_id").toHexString();
        }else{
            this.id = o.getObjectId("_id").toHexString();
            this.data = o.toJson();
        }
        this.offset = document.get("ts", BsonTimestamp.class).getValue();
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
