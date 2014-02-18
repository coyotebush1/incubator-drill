package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Jackson serializable description of a file selection. Maintains an internal set of file statuses. However, also
 * serializes out as a list of Strings. All accessing methods first regenerate the FileStatus objects if they are not
 * available.  This allows internal movement of FileStatus and the ability to serialize if need be.
 */
public class FileSelection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);

  @JsonIgnore
  private List<FileStatus> statuses;

  @JsonProperty
  public List<String> paths;

  public FileSelection() {
  }

  public FileSelection(List<FileStatus> statuses) {
    this.statuses = statuses;
    this.paths = Lists.newArrayList();
    for (FileStatus f : statuses) {
      paths.add(f.getPath().toString());
    }
  }

  public boolean containsDirectories(DrillFileSystem fs) throws IOException {
    init(fs);
    for (FileStatus p : statuses) {
      if (p.isDir()) return true;
    }
    return false;
  }

  public FileSelection minusDirectorries(DrillFileSystem fs) throws IOException {
    init(fs);
    List<FileStatus> newList = Lists.newArrayList();
    for (FileStatus p : statuses) {
      if (p.isDir()) {
        List<FileStatus> statuses = fs.list(true, p.getPath());
        for (FileStatus s : statuses) {
          newList.add(s);
        }

      } else {
        newList.add(p);
      }
    }
    return new FileSelection(newList);
  }

  public FileStatus getFirstPath(DrillFileSystem fs) throws IOException {
    init(fs);
    return statuses.get(0);
  }

  private void init(DrillFileSystem fs) throws IOException {
    if (paths != null && statuses == null) {
      statuses = Lists.newArrayList();
      for (String p : paths) {
        statuses.add(fs.getFileStatus(new Path(p)));
      }
    }
  }

  public List<FileStatus> getFileStatusList(DrillFileSystem fs) throws IOException {
    init(fs);
    return statuses;
  }

  public static FileSelection create(DrillFileSystem fs, Path parent, String path) throws IOException {
    if (Pattern.quote(path).equals(path)) {
      Path p = new Path(parent, path);
      FileStatus status = fs.getFileStatus(p);
      return new FileSelection(Collections.singletonList(status));
    } else {
      FileStatus[] status = fs.getUnderlying().globStatus(new Path(parent, path));
      return new FileSelection(Lists.newArrayList(status));
    }
  }

}
