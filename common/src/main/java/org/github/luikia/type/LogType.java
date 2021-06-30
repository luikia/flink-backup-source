package org.github.luikia.type;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;

public enum LogType {

    INSERT("insert"), DELETE("delete"), UPDATE("update"), OTHER("other");
    private String name;

    LogType(String name) {
        this.name = name;
    }

    public static LogType getLogType(String name) {
        Optional<LogType> type = Arrays.stream(LogType.values()).filter(e -> StringUtils.equalsIgnoreCase(e.name, name)).findFirst();
        return type.orElse(OTHER);
    }

    public static LogType getLogTypePrefix(String prefix) {
        Optional<LogType> type = Arrays.stream(LogType.values()).filter(e -> StringUtils.startsWithIgnoreCase(e.name, prefix)).findFirst();
        return type.orElse(OTHER);
    }
}
