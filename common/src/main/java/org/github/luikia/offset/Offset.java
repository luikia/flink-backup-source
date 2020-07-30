package org.github.luikia.offset;

import com.google.gson.Gson;

import java.io.Serializable;

public abstract class Offset implements Serializable {

    protected static final Gson g = new Gson();

    public abstract String toJsonString();

}
