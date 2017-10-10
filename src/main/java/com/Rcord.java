package com;

import org.apache.avro.reflect.Nullable;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hailingu on 12/14/15.
 */
public class Rcord {

    public int lineNu;

    @Nullable
    public String content;

    public Map<String, String> map;

    public Rcord() {

    }

    public Rcord(int i, String c) {
        map = new HashMap<String, String>();
        lineNu = i;
        content = c;
        map.put("k", c);
    }

    @Override
    public String toString() {
        return "" + lineNu + ", "  + content + ", " + map.get("k");
    }
}
