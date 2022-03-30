package org.github.luikia.oplog.format;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;

@Data
public class ChangeStreamData implements Serializable {

    public static final TypeInformation<ChangeStreamData> TYPES = Types.POJO(ChangeStreamData.class, ImmutableMap.<String, TypeInformation<?>>builder()
            .put("documentKey", Types.STRING)
            .put("operationType", Types.STRING)
            .put("fullDocument", Types.STRING)
            .put("resumeToken", Types.STRING)
            .put("db", Types.STRING)
            .put("collection", Types.STRING)
            .put("ts", Types.LONG)
            .build());

    private static final long serialVersionUID = 1L;

    private String documentKey;

    private String db;

    private String collection;

    private String operationType;

    private String fullDocument;

    private String resumeToken;

    private Long ts = 0L;
}
