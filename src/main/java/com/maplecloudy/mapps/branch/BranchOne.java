package com.maplecloudy.mapps.branch;

import java.util.HashMap;

import org.spark_project.guava.collect.Maps;

import com.maplecloudy.app.MAppRunner;
import com.maplecloudy.app.MAppTool;
import com.maplecloudy.app.annotation.Action;
import com.maplecloudy.app.utils.MAppUtils;

@Action
public class BranchOne implements MAppTool {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  public static void main(String[] args) throws Exception {
    System.exit(MAppRunner.run(new BranchOne(), args));
  }
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("*************分支One程序启动**********");
    String parameter = MAppUtils.getParameter("branchOne", "branchOne");
    System.out.println(
        "*************branchOne获得的输入为（默认是branchOne）**********" + parameter);
    HashMap<String,String> newHashMap = Maps.newHashMap();
    newHashMap.put("selector", "branchOne");
    MAppUtils.savePipelineOutput(newHashMap);
    return 0;
  }
}
