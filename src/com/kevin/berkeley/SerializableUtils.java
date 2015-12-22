package com.kevin.berkeley;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SerializableUtils<T> {
  public byte[] changeToBytes(T o) {
    byte[] result = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(new GZIPOutputStream(bos));
      oos.writeObject(o);
      oos.close();
      result = bos.toByteArray();
    } catch (Exception ex) {
      ex.printStackTrace();
      return null;
    } finally {
      try {
        if (null != oos)
          oos.close();
        if (bos != null)
          bos.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public T changeToObj(byte[] bytes) {
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)));
      return (T) ois.readObject();
    } catch (Exception ex) {
      ex.printStackTrace();
      return null;
    } finally {
      try {
        if (null != ois) {
          ois.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
