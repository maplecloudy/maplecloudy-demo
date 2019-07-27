package com.maplecloudy.mapps.branch;

import java.util.HashMap;

import org.spark_project.guava.collect.Maps;

import com.maplecloudy.app.MAppRunner;
import com.maplecloudy.app.MAppTool;
import com.maplecloudy.app.annotation.Action;
import com.maplecloudy.app.utils.MAppUtils;

@Action
public class BranchTwo implements MAppTool {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  public static void main(String[] args) throws Exception {
    System.exit(MAppRunner.run(new BranchTwo(), args));
  }
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("*************分支Two程序启动**********");
    String parameter = MAppUtils.getParameter("branchTow", "branchTwo");
    System.out.println(
        "*************branchTwo获得的输入为（默认是branchTwo）**********" + parameter);
    HashMap<String,String> newHashMap = Maps.newHashMap();
    newHashMap.put("selector", "branchTwo");
    MAppUtils.savePipelineOutput(newHashMap);
    return 0;
  }
}
