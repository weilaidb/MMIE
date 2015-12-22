package com.kevin.berkeley;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class DBEnvironment {
  private static Environment environment = null;
  public static String DB_ENVIRONMENT_ROOT = "/data/berkeleyDB/";
  private static EnvironmentConfig config = new EnvironmentConfig();
  static {
    try {
      init();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void initDirectory() throws IOException {
    File envDir = new File(DB_ENVIRONMENT_ROOT);
    if (!envDir.exists()) {
      if (!envDir.mkdirs()) {
        throw new IOException("create directory " + DB_ENVIRONMENT_ROOT + " fail");
      }
    } else if (envDir.isFile()) {
      throw new IOException(DB_ENVIRONMENT_ROOT + " must be a directory");
    }
  }

  private static void init() throws IOException {
    initDirectory();
    config.setAllowCreate(true);// 如果不存在则创建一个
    config.setReadOnly(false); // 以只读方式打开，默认为false
    config.setTransactional(true); // 事务支持.默认为false
    config.setCachePercent(20);// 设置当前环境能够使用的RAM占整个JVM内存的百分比。
    config.setTxnNoSyncVoid(false);// 当提交事务的时候是否把缓存中的内容同步到磁盘中去。true
    // 表示不同步，也就是说不写磁盘
    config.setTxnWriteNoSyncVoid(false);// 当提交事务的时候，是否把缓冲的log写到磁盘上true
    // 表示不同步，也就是说不写磁盘
    environment = new Environment(new File(DB_ENVIRONMENT_ROOT), config);
    environment.sync();// 据同步到磁盘
  }

  public static Environment getEnvironment() {
    return environment;
  }

  public static void destroy() {
    if (environment != null) {
      environment.cleanLog();
      environment.close();
    }
  }

  public static void sync() {
    if (environment != null) {
      environment.sync();
    }
  }
}
