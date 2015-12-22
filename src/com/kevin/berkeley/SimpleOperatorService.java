package com.kevin.berkeley;

import java.io.UnsupportedEncodingException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class SimpleOperatorService {
  private Database database;

  public SimpleOperatorService(Database database) {
    this.database = database;
  }

  // 向database中添加一条记录。如果你的database不支持一个key对应多个data或当前database中已经存在该key了，则使用此方法将使用新的值覆盖旧的值。
  public boolean put(String key, String value) {
    DatabaseEntry theKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry theDate = new DatabaseEntry(value.getBytes());
    OperationStatus status = database.put(null, theKey, theDate);
    if (status == OperationStatus.SUCCESS) {
      return true;
    }
    return false;
  }

  /**
   * 向database中添加新值但如果原先已经有了该key，则不覆盖。 不管database是否允许支持多重记录(一个key对应多个value),
   * 只要存在该key就不允许添加，并且返回perationStatus.KEYEXIST信息。
   * 
   * @param key
   * @param value
   * @return
   */
  public boolean putNotExists(String key, String value) {
    DatabaseEntry theKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry theDate = new DatabaseEntry(value.getBytes());
    OperationStatus status = database.putNoOverwrite(null, theKey, theDate);
    if (status == OperationStatus.SUCCESS) {
      return true;
    }
    return false;
  }

  /**
   * 想database中添加一条记录，如果database中已经存在了相同的 key和value则返回 OperationStatus.KEYEXIST.
   * 
   * @return
   */
  public boolean putNoDupData(String key, String value) {
    DatabaseEntry theKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry theDate = new DatabaseEntry(value.getBytes());
    OperationStatus status = database.putNoDupData(null, theKey, theDate);
    if (status == OperationStatus.SUCCESS) {
      return true;
    }
    return false;
  }

  public String get(String key) {
    DatabaseEntry theKey;
    theKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry value = new DatabaseEntry();
    OperationStatus status = database.get(null, theKey, value, LockMode.READ_COMMITTED);
    if (status == OperationStatus.SUCCESS) {
      return new String(value.getData());
    }
    return null;
  }

  // 通过key和value来同时匹配，同样如果没有记录匹配key和value则会返回OperationStatus.NOTFOUND
  public boolean searchKeyAndValue(String key, String value) {
    DatabaseEntry theKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry theValue = new DatabaseEntry(value.getBytes());
    OperationStatus status = database.getSearchBoth(null, theKey, theValue, LockMode.DEFAULT);
    if (status == OperationStatus.SUCCESS) {
      return true;
    }
    return false;
  }

  public boolean delete(String key) {
    return (database.delete(null, new DatabaseEntry(key.getBytes())) == OperationStatus.SUCCESS);
  }

  // 不同类型的数据的处理
  // 你可以使用DatabaseEntry来绑定基本的JAVA数据类型，主要有String、Character、Boolean、Byte、Short、Integer、Long、Float、Double.
  public boolean putPrimitiveType(String key, Long longValue) {
    DatabaseEntry databaseEntry = new DatabaseEntry(key.getBytes());
    DatabaseEntry entryValue = new DatabaseEntry();
    EntryBinding<Long> entryBinding = TupleBinding.getPrimitiveBinding(Long.class);
    entryBinding.objectToEntry(longValue, entryValue);
    return OperationStatus.SUCCESS.compareTo(database.put(null, databaseEntry, entryValue)) == 0;
  }

  public Long getPrimitiveType(String key) {
    DatabaseEntry theKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry theData = new DatabaseEntry();
    EntryBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
    OperationStatus retVal = database.get(null, theKey, theData, LockMode.DEFAULT);
    if (retVal == OperationStatus.SUCCESS) {
      return myBinding.entryToObject(theData);
    }
    return null;
  }

  // 让database使用你自定义的比较器
  // 如果你想改变database中基本的排序方式，你只能重新创建database并重新导入数据。
  // ①. DatabaseConfig.setBtreeComparator()
  // 用于在database里两个key的比较
  // ②. DatabaseConfig.setOverrideBtreeComparator()
  // 如果为true则代表让database使用 DatabaseConfig.setBtreeComparator()设置的比较器来代替默认的比较器。
  // ③. DatabaseConfig.setDuplicateComparator()
  // 用于database可以使用多重记录的时候的data的 比较。
  // ④. DatabaseConfig.setOverrideDuplicateComparator()
  // 如果为true则代表让database使用 DatabaseConfig. setDuplicateComparator()设置 的比
  // 较器来代替默认的比较器。

  public void getNext() {
    Cursor cursor = null;
    try {// 打开游标
      cursor = database.openCursor(null, null);
      DatabaseEntry foundKey = new DatabaseEntry();
      DatabaseEntry foundData = new DatabaseEntry();
      // 通过cursor.getNex方法来遍历记录
      while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        String keyString = new String(foundKey.getData(), "UTF-8");
        String dataString = new String(foundData.getData(), "UTF-8");
        System.out.println("Key | Data : " + keyString + " | " + dataString + "");
      }
    } catch (DatabaseException de) {
      de.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } finally {
      // 使用后必须关闭游标
      cursor.close();
    }
  }

  public void getPrev() {
    Cursor cursor = null;
    try {
      cursor = database.openCursor(null, null);
      DatabaseEntry foundKey = new DatabaseEntry();
      DatabaseEntry foundData = new DatabaseEntry("Test1".getBytes("UTF-8"));
      // 使用cursor.getPrev方法来遍历游标获取数据
      while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        String theKey = new String(foundKey.getData(), "UTF-8");
        String theData = new String(foundData.getData(), "UTF-8");
        System.out.println("Key | Data : " + theKey + " | " + theData + "");
      }
    } catch (DatabaseException de) {
      de.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } finally {
      // 使用后必须关闭游标
      cursor.close();
    }
  }

  // 搜索数据
  // 你可以通过游标方式搜索你的database记录，你也可以通过一个key来搜索你的记录，同样的你也可以通过key和value组合在一起来搜索记录。如果查询失败，则游标会返回OperationStatus.NOTFOUND。
  // 游标支持都检索方法如下：
  // 1) Cursor.getSearchKey()
  // 通过key的方式检索，使用后游标指针将移动到跟当前key匹配的第一项。
  // 2) Cursor.getSearchKeyRange()
  // 把游标移动到大于或等于查询的key的第一个匹配key,大小比较是通过你设置的比较器来完成的，如果没有设置则使用默认的比较器。
  // 3) Cursor.getSearchBoth()
  // 通过key和value方式检索，然后把游标指针移动到与查询匹配的第一项。
  // 4) Cursor.getSearchBothRange()
  // 把游标移动到所有的匹配key和大于或等于指定的data的第一项。
  // 比如说database存在如下的key/value记录，,大小比较是通过你设置的比较器来完成的

  public String getSearchKey(String key) {
    Cursor cursor = null;
    try {
      cursor = database.openCursor(null, null);
      DatabaseEntry thekey = new DatabaseEntry(key.getBytes("UTF-8"));
      DatabaseEntry data = new DatabaseEntry();
      OperationStatus operationStatus = cursor.getSearchKey(thekey, data, LockMode.DEFAULT);
      if (operationStatus == OperationStatus.NOTFOUND) {
        return null;
      }
      return new String(data.getData(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
    return null;
  }

  // l 通过游标来添加数据
  // 你可以通过游标来向database里添加数据
  // 你可以使用如下方法来向database里添加数据
  // 1) Cursor.put()
  // 如果database不存在key,则添加，如果database存在key但允许多重记录，则可以通过比较器在适当的位置插入数据，如果key已存在且不支持多重记录，则替换原有的数据。
  // 2) Cursor.putNoDupData()
  // 如果存在相同的key和data则返回OperationStatus.KEYEXIST.
  // 如果不存在key则添加数据。
  // 3) Cursor.putNoOverwrite()
  // 如果存在相同的key在database里则返OperationStatus.KEYEXIS，
  // 如果不存在key则添加数据。

  public void useCursorPut(String key, String value) {
    Cursor cursor = database.openCursor(null, null);
    try {
      DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
      DatabaseEntry theData = new DatabaseEntry(value.getBytes("UTF-8"));
      cursor.put(theKey, theData);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } finally {
      cursor.close();
    }
  }

  public boolean deleteUseCursor(String key) {
    Cursor cursor = database.openCursor(null, null);
    try {
      DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
      DatabaseEntry theData = new DatabaseEntry();
      OperationStatus operationStatus = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
      if (operationStatus == OperationStatus.SUCCESS) {
        operationStatus = cursor.delete();
        return (operationStatus == OperationStatus.SUCCESS);
      }
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } finally {
      cursor.close();
    }
    return false;
  }

  public void updateUseCursor(String key, String replaceStr) {
    Cursor cursor = null;
    try {
      DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
      DatabaseEntry theData = new DatabaseEntry();
      cursor = database.openCursor(null, null);
      cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
      cursor.delete();
      DatabaseEntry replacementData = new DatabaseEntry(replaceStr.getBytes("UTF-8"));
      cursor.put(theKey, replacementData);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cursor.close();
    }
  }

  public void put(byte key[], byte[] value) {
    DatabaseEntry keyEntry = new DatabaseEntry(key);
    DatabaseEntry valueEntry = new DatabaseEntry(value);
    this.database.put(null, keyEntry, valueEntry);
  }

  public byte[] get(byte[] key) {
    DatabaseEntry keyEntry = new DatabaseEntry(key);
    DatabaseEntry valueEntry = new DatabaseEntry();
    OperationStatus status = this.database.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
    if (status == OperationStatus.SUCCESS) {
      return valueEntry.getData();
    }
    return null;
  }

}
