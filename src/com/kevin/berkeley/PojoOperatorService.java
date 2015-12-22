package com.kevin.berkeley;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class PojoOperatorService<T> {
  private Database database;
  private SerializableUtils<T> serializableUtils = new SerializableUtils<T>();

  public PojoOperatorService(Database database) {
    this.database = database;
  }

  public boolean put(String key, T o) {
    DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
    DatabaseEntry valueEntry = new DatabaseEntry(serializableUtils.changeToBytes(o));
    OperationStatus status = this.database.put(null, keyEntry, valueEntry);
    return status.compareTo(OperationStatus.SUCCESS) == 0;
  }

  public T get(String key) {
    DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
    DatabaseEntry valueEntry = new DatabaseEntry();
    OperationStatus status = this.database.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
    if (status == OperationStatus.SUCCESS) {
      return (T) serializableUtils.changeToObj(valueEntry.getData());
    }
    return null;
  }

  public boolean delete(String key) {
    DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
    OperationStatus status = this.database.delete(null, keyEntry);
    return status.compareTo(OperationStatus.SUCCESS) == 0;
  }

}
