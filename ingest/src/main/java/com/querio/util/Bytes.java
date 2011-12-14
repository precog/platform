package com.querio.util;

import java.nio.ByteBuffer;

public final class Bytes {
  public static final byte[] add(Iterable<byte[]> toAdd) {
    int max = 0;
    for (byte[] a : toAdd) max = (max > a.length) ? max : a.length;
    byte[] target = new byte[max];
    for (byte[] a : toAdd) {
      for (int i = 0; i < a.length; i++) target[i] = (byte) (target[i] + a[i]);
    }
    return target;
  }
}
