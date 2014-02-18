package org.apache.drill.exec.store.dfs;

public class FilePath {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilePath.class);
  
  public String path;
  public boolean isDirectory;

  public FilePath(String path, boolean isDirectory) {
    this.path = path;
    this.isDirectory = isDirectory;
  }
  
}
