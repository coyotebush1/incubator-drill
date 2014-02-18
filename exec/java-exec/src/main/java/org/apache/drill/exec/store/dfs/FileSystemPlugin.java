package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.hadoop.fs.FileSystem;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * A Storage engine associated with a Hadoop FileSystem Implementation. Examples include HDFS, MapRFS, QuantacastFileSystem,
 * LocalFileSystem, as well Apache Drill specific CachedFileSystem, ClassPathFileSystem and LocalSyncableFileSystem.
 * Tables are file names, directories and path patterns. This storage engine delegates to FSFormatEngines but shares
 * references to the FileSystem configuration and path management.
 */
public class FileSystemPlugin extends AbstractStoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemPlugin.class);

  private FileSystemSchemaFactory schemaFactory;
  private FileSystem fs;
  private Map<Pattern, FormatPlugin> fileMatchers;
  private Map<String, FormatPlugin> enginesByName;
  private Map<FormatPluginConfig, FormatPlugin> formatPluginsByConfig;
  private FileSystemConfig config;
  private DrillbitContext context;
  
  public FileSystemPlugin(FileSystemConfig config, DrillbitContext context, String name){
    WorkspaceSchemaFactory[] factories = null;
    this.schemaFactory = new FileSystemSchemaFactory(name, factories);
    this.context = context;
  }
  
  @Override
  public boolean supportsRead() {
    return true;
  }
  
  @Override
  public AbstractGroupScan getPhysicalScan(Scan scan) throws IOException {
    FormatSelection formatSelection = scan.getSelection().getWith(context.getConfig(), FormatSelection.class);
    FormatPlugin plugin = enginesByName.get(formatSelection.format);
    if(plugin == null) throw new IOException(String.format("Failure getting requested format plugin named '%s'.  It was not one of the format plugins registered.", formatSelection.format));
    return plugin.getGroupScan(scan.getOutputReference(), formatSelection.selection);
  }
  
  @Override
  public void createAndAddSchema(SchemaPlus parent) {
    schemaFactory.add(parent);
  }
  
  public FormatPlugin getFormatPlugin(String name){
    return enginesByName.get(name);
  }
  
  public FormatPlugin getFormatPlugin(FormatPluginConfig config){
    if(config instanceof NamedFormatPluginConfig){
      return enginesByName.get(((NamedFormatPluginConfig) config).name);
    }else{
      return formatPluginsByConfig.get(config);
    }
  }

  public FormatPlugin getDefaultPlugin(){
    return enginesByName.get(config.defaultFormat);
  }
}
