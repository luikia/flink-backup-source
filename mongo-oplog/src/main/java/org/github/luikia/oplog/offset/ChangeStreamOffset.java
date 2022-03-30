package org.github.luikia.oplog.offset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.github.luikia.offset.Offset;

@Data
@NoArgsConstructor
public class ChangeStreamOffset extends Offset {

    private static final long serialVersionUID = 1L;

    private String resumeToken;

    public ChangeStreamOffset(String resumeToken) {
        this.resumeToken = resumeToken;
    }

    @Override
    public String toJsonString() {
        return g.toJson(this);
    }

    public static ChangeStreamOffset fromJson(String json) {
        return g.fromJson(json, ChangeStreamOffset.class);
    }
}
