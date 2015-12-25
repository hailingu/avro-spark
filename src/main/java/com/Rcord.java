package com;

import org.apache.avro.reflect.Nullable;

/**
 * Created by hailingu on 12/14/15.
 */
public class Rcord {

  public int lineNu;

  @Nullable
  public String content;

  public Rcord() {}

  public Rcord(int i, String c){
    lineNu = i;
    content = c;
  }
}
