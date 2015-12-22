package com.kevin.berkeley;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;

public class BDBManager {
  private static Map<String, Database> databaseInstanceMap = new HashMap<String, Database>();

  public static Database getDatabase(String name) {
    if (databaseInstanceMap.containsKey(name)) {
      return databaseInstanceMap.get(name);
    }
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setSortedDuplicates(true);
    // 设置用于Btree比较的比较器，通常是用来排序
    dbConfig.setBtreeComparator(new BTreeComparator<byte[]>());
    // 设置用来比较一个key有两个不同值的时候的大小比较器
    dbConfig.setDuplicateComparator(new DuplicateComparatorClass<byte[]>());
    // 设置一个key是否允许存储多个值，true代表允许，默认false.
    dbConfig.setSortedDuplicates(true);
    // 以独占的方式打开，也就是说同一个时间只能有一实例打开这个database。
    dbConfig.setExclusiveCreate(false);
    Database database = DBEnvironment.getEnvironment().openDatabase(null, name, dbConfig);
    databaseInstanceMap.put(name, database);
    return database;
  }

  public static void removeDatabase(String name) {
    DBEnvironment.getEnvironment().removeDatabase(null, name);
  }

  public static void close(String name) {
    Database database = databaseInstanceMap.remove(name);
    if (database != null) {
      database.close();
    }
  }

  public static void closeAll() {
    Set<String> set = databaseInstanceMap.keySet();
    for (String key : set) {
      databaseInstanceMap.get(key).close();
    }
  }

  public static long truncateDatabase(String database) {
    return DBEnvironment.getEnvironment().truncateDatabase(null, database, true);
  }

  public static void truncateDatabase() {
    Set<String> set = databaseInstanceMap.keySet();
    for (String key : set) {
      truncateDatabase(key);
    }
  }

  public static void syncAll() {
    for (Map.Entry<String, Database> entry : databaseInstanceMap.entrySet()) {
      entry.getValue().sync();
    }
  }

  public static void sync(String database) {
    if (databaseInstanceMap.containsKey(database)) {
      databaseInstanceMap.get(database).sync();
    }
  }
}
