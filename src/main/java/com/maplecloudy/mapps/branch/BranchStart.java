package com.maplecloudy.mapps.branch;

import java.util.HashMap;
import java.util.Random;

import org.spark_project.guava.collect.Maps;

import com.maplecloudy.app.MAppRunner;
import com.maplecloudy.app.MAppTool;
import com.maplecloudy.app.annotation.Action;
import com.maplecloudy.app.utils.MAppUtils;

@Action
public class BranchStart implements MAppTool {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  public static void main(String[] args) throws Exception {
    System.exit(MAppRunner.run(new BranchStart(), args));
  }
  
  @Override
  public int run(String[] args) throws Exception {
    int nextInt = Math.abs(new Random().nextInt());
    HashMap<String,Integer> newHashMap = Maps.newHashMap();
    System.out.println("生成的随机数是：" + nextInt);
    System.out.println("随机数对3取余后的值：" + nextInt % 3);
    newHashMap.put("selector", nextInt % 3);
    MAppUtils.savePipelineOutput(newHashMap);
    return 0;
  }
}
