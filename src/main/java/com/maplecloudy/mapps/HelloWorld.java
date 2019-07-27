package com.maplecloudy.mapps;

import java.util.HashMap;

import org.spark_project.guava.collect.Maps;

import com.maplecloudy.app.MAppRunner;
import com.maplecloudy.app.MAppTool;
import com.maplecloudy.app.annotation.Action;
import com.maplecloudy.app.utils.MAppUtils;

@Action
public class HelloWorld implements MAppTool {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  public static void main(String[] args) throws Exception {
    System.exit(MAppRunner.run(new HelloWorld(), args));
  }
  
  @Override
  public int run(String[] args) throws Exception {
    HashMap<String,String> newHashMap = Maps.newHashMap();
    System.out.println("hello,world");
    newHashMap.put("output", "hello,world");
    MAppUtils.savePipelineOutput(newHashMap);
    return 0;
  }
}
