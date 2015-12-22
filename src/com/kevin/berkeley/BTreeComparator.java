package com.kevin.berkeley;

import java.io.Serializable;
import java.util.Comparator;

public class BTreeComparator<T> implements Comparator<byte[]>, Serializable {

  private static final long serialVersionUID = 8106891884691735416L;

  @Override
  public int compare(byte[] b1, byte[] b2) {
    return new String(b1).compareTo(new String(b2));
  }
}
