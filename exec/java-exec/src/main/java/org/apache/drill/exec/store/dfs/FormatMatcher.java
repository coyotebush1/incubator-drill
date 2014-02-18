package org.apache.drill.exec.store.dfs;

import java.io.IOException;

public abstract class FormatMatcher {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatMatcher.class);

  public abstract boolean supportDirectoryReads();
  public abstract FormatSelection isReadable(FileSelection file) throws IOException;
  public abstract FormatPlugin getFormatPlugin();
}
