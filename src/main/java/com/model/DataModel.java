package com.model;

import lombok.Data;
import org.apache.avro.reflect.Nullable;

import java.util.List;


/**
 * Created by hailingu on 12/14/15.
 */
@Data
public class DataModel {

    public int i;

    @Nullable
    public String s;

    public double d;

    @Nullable
    public List<String> list;

    public DataModel() {

    }
}
