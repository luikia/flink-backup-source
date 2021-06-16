package org.github.luikia.type;

public enum LogType {

    INSERT("insert"), DELETE("delete"), UPDATE("update"), OTHER("other");
    private String name;

    LogType(String name) {
        this.name = name;
    }
}
