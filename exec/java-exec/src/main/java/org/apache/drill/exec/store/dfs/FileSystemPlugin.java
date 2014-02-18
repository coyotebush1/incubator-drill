package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.dfs.shim.FileSystemCreator;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;

/**
 * A Storage engine associated with a Hadoop FileSystem Implementation. Examples include HDFS, MapRFS, QuantacastFileSystem,
 * LocalFileSystem, as well Apache Drill specific CachedFileSystem, ClassPathFileSystem and LocalSyncableFileSystem.
 * Tables are file names, directories and path patterns. This storage engine delegates to FSFormatEngines but shares
 * references to the FileSystem configuration and path management.
 */
public class FileSystemPlugin extends AbstractStoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemPlugin.class);

  private final FileSystemSchemaFactory schemaFactory;
  private Map<String, FormatPlugin> formatsByName;
  private Map<FormatPluginConfig, FormatPlugin> formatPluginsByConfig;
  private FileSystemConfig config;
  private DrillbitContext context;
  private final DrillFileSystem fs;
  
  public FileSystemPlugin(FileSystemConfig config, DrillbitContext context, String name) throws ExecutionSetupException{
    try{
      this.config = config;
      this.context = context;
      
      Configuration fsConf = new Configuration();
      fsConf.set("fs.default.name", config.connection);
      this.fs = FileSystemCreator.getFileSystem(context.getConfig(), fsConf);
      this.formatsByName = FormatCreator.getFormatPlugins(context, fs, config);
      List<FormatMatcher> matchers = Lists.newArrayList();
      formatPluginsByConfig = Maps.newHashMap();
      for(FormatPlugin p : formatsByName.values()){
        matchers.add(p.getMatcher());
        formatPluginsByConfig.put(p.getConfig(), p);
      }
      
      List<WorkspaceSchemaFactory> factories = null;
      if(config.workspaces == null || config.workspaces.isEmpty()){
        factories = Collections.singletonList(new WorkspaceSchemaFactory("default", name, fs, "/", matchers));
      }else{
        factories = Lists.newArrayList();
        for(Map.Entry<String, String> space : config.workspaces.entrySet()){
          factories.add(new WorkspaceSchemaFactory(space.getKey(), name, fs, space.getValue(), matchers));
        }
      }
      this.schemaFactory = new FileSystemSchemaFactory(name, factories);
    }catch(IOException e){
      throw new ExecutionSetupException("Failure setting up file system plugin.", e);
    }
  }
  
  @Override
  public boolean supportsRead() {
    return true;
  }
  
  @Override
  public AbstractGroupScan getPhysicalScan(Scan scan) throws IOException {
    FormatSelection formatSelection = scan.getSelection().getWith(context.getConfig(), FormatSelection.class);
    FormatPlugin plugin;
    if(formatSelection.format instanceof NamedFormatPluginConfig){
      plugin = formatsByName.get( ((NamedFormatPluginConfig) formatSelection.format).name);
    }else{
      plugin = formatPluginsByConfig.get(formatSelection.format);
    }
    if(plugin == null) throw new IOException(String.format("Failure getting requested format plugin named '%s'.  It was not one of the format plugins registered.", formatSelection.format));
    return plugin.getGroupScan(scan.getOutputReference(), formatSelection.selection);
  }
  
  @Override
  public void createAndAddSchema(SchemaPlus parent) {
    schemaFactory.add(parent);
  }
  
  public FormatPlugin getFormatPlugin(String name){
    return formatsByName.get(name);
  }
  
  public FormatPlugin getFormatPlugin(FormatPluginConfig config){
    if(config instanceof NamedFormatPluginConfig){
      return formatsByName.get(((NamedFormatPluginConfig) config).name);
    }else{
      return formatPluginsByConfig.get(config);
    }
  }

}
