package org.github.luikia.oplog.format;

import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.io.Serializable;

@Data
public class OplogData implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final TypeInformation<OplogData> TYPE = Types.GENERIC(OplogData.class);

    private Long offset;

    private String namespace;

    private String type;

    private String data;

    private String id;

    private String document;

    public OplogData(){

    }

    public OplogData(Document document) {
        this.document = document.toJson();
        this.namespace = document.getString("ns");
        this.type = getType(document.getString("op"));
        Document o = document.get("o", Document.class);
        this.id = o.getObjectId("_id").toHexString();
        this.offset = document.get("ts", BsonTimestamp.class).getValue();
        this.data = o.toJson();
    }

    public OplogData(String json) {
        this(Document.parse(json));
    }

    private String getType(String op) {
        String type = null;
        switch (op) {
            case "i": {
                type = "insert";
                break;
            }
            case "u": {
                type = "update";
                break;
            }
            case "d": {
                type = "delete";
                break;
            }

        }
        return type;
    }

    public Document getDocument() {
        return Document.parse(this.document);
    }

    @Override
    public String toString() {
        return document;
    }
}
