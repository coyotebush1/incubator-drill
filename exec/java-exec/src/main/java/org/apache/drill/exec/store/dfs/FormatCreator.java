package org.apache.drill.exec.store.dfs;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;

import com.google.common.collect.Maps;

public class FormatCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatCreator.class);
  
  static Map<String, FormatPlugin> getFormatPlugins(DrillbitContext context, DrillFileSystem fileSystem, FileSystemConfig storageConfig){
    final DrillConfig config = context.getConfig(); 
    Map<String, FormatPlugin> plugins = Maps.newHashMap();

    Collection<Class<? extends FormatPlugin>> pluginClasses = PathScanner.scanForImplementations(FormatPlugin.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));

    
    if(storageConfig.formats == null || storageConfig.formats.isEmpty()){
      
      for(Class<? extends FormatPlugin> pluginClass: pluginClasses){
        for(Constructor<?> c : pluginClass.getConstructors()){
          try{
            Class<?>[] params = c.getParameterTypes();
            if(params.length != 3) continue;
            FormatPlugin plugin = (FormatPlugin) c.newInstance(context, fileSystem, storageConfig);
            plugins.put(plugin.getDefaultName(), plugin);
          }catch(Exception e){
            logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
          }
        }
      }
      
    }else{
      
      Map<Class<?>, Constructor<?>> constructors = Maps.newHashMap();
      for(Class<? extends FormatPlugin> pluginClass: pluginClasses){
        for(Constructor<?> c : pluginClass.getConstructors()){
          try{
            Class<?>[] params = c.getParameterTypes();
            if(params.length != 4) continue;
            constructors.put(pluginClass, c);
          }catch(Exception e){
            logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
          }
        }
      }
      
      for(Map.Entry<String, FormatPluginConfig> e : storageConfig.formats.entrySet()){
        Constructor<?> c = constructors.get(e.getValue().getClass());
        if(c == null){
          logger.warn("Unable to find constructor for storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName());
          continue;
        }
        try{
        plugins.put(e.getKey(), (FormatPlugin) c.newInstance(context, fileSystem, storageConfig, e.getValue()));
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
          logger.warn("Failure initializing storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName(), e1);
        }
      }
      
      
    }
    
    return plugins;
  }
  
  

}
