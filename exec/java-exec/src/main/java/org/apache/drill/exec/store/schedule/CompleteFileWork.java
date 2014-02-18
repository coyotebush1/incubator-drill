package org.apache.drill.exec.store.schedule;

import org.apache.drill.exec.store.dfs.easy.FileWork;

public class CompleteFileWork implements FileWork, CompleteWork{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompleteFileWork.class);

  private long start;
  private long length;
  private String path;
  private EndpointByteMap byteMap;
  
  public CompleteFileWork(EndpointByteMap byteMap, long start, long length, String path) {
    super();
    this.start = start;
    this.length = length;
    this.path = path;
  }

  @Override
  public int compareTo(CompleteWork o) {
    return Long.compare(getTotalBytes(), o.getTotalBytes());
  }

  @Override
  public long getTotalBytes() {
    return length;
  }

  @Override
  public EndpointByteMap getByteMap() {
    return byteMap;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public long getStart() {
    return start;
  }

  @Override
  public long getLength() {
    return length;
  }
  
  public FileWorkImpl getAsFileWork(){
    return new FileWorkImpl(start, length, path);
  }
  
  public static class FileWorkImpl implements FileWork{

    
    public FileWorkImpl(long start, long length, String path) {
      super();
      this.start = start;
      this.length = length;
      this.path = path;
    }

    public long start;
    public long length;
    public String path;
    
    @Override
    public String getPath() {
      return null;
    }

    @Override
    public long getStart() {
      return 0;
    }

    @Override
    public long getLength() {
      return 0;
    }
    
  }
}
