package com.maplecloudy.mapps.branch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.spark_project.guava.collect.Maps;

import com.google.gson.Gson;
import com.maplecloudy.api.AppConstant;
import com.maplecloudy.api.app.AppPod;
import com.maplecloudy.app.MAppRunner;
import com.maplecloudy.app.MAppTool;
import com.maplecloudy.app.annotation.Action;
import com.maplecloudy.app.utils.MAppUtils;

import scala.Tuple2;

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
    // 参数个数检查  如果输入一个则转换成int 否则切割二次
    int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<Integer>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }
    System.out.println("******************开始加载用户设置的程序参数******************************");
    HashMap<String,Object> parameter = MAppUtils.getAppPod().getParameter();
    System.out.println("全部输入参数：" + new Gson().toJson(parameter));
    
    System.out.println("******************开始加载spark参数，使用用户设置的参数进行覆盖**************");
    MAppUtils.loadSparkConf();
    SparkConf sparkConf = new SparkConf(true).setAppName("JavaSparkPi");
    MAppUtils.appendHiveConf2Spark(sparkConf);
    System.out.println("******************打印spark所有参数内容*********************************");
    for (Tuple2<String,String> tp : sparkConf.getAll()) {
      System.out.println(tp._1 + ":" + tp._2);
    }
    System.out.println("******************打印所有系统环境变量参数*********************************");
    Map<String,String> getenv = System.getenv();
    for (Map.Entry<String,String> env : getenv.entrySet()) {
      System.out.println(
          "System env: key=" + env.getKey() + ", val=" + env.getValue());
    }
    
    System.out.println("******************获得*********************************");
    AppPod appPod = MAppUtils.getAppPod();
    System.out.println("spark pi输出apppod信息");
    if (appPod != null) {
      System.out
          .println("appPod信息：" + AppConstant.om.writeValueAsString(appPod));
    } else {
      System.out.println("appPod is null");
    }
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    
    Configuration conf = jsc.hadoopConfiguration();
    System.out.println("conf 长度:" + conf.size());
    conf.forEach(s -> {
      System.out.println("hadoop conf:" + s.getKey() + s.getValue());
    });
    System.out.println("hadoop conf key:");
    MAppUtils.saveSparkContext(jsc);
    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
    int count = dataSet.map(new Function<Integer,Integer>() {
      @Override
      public Integer call(Integer integer) {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;
        return (x * x + y * y < 1) ? 1 : 0;
      }
    }).reduce(new Function2<Integer,Integer,Integer>() {
      
      @Override
      public Integer call(Integer integer, Integer integer2) {
        return integer + integer2;
      }
    });
    System.out.println("Pi is roughly " + 4.0 * count / n);
    jsc.stop();
//    Thread.sleep(1000 * 60);
    MAppUtils.savePipelineOutput("pi is roughly " + 4.0 * count / n);
    HashMap<String,String> newHashMap = Maps.newHashMap();
    newHashMap.put("selector", "branch2");
    MAppUtils.savePipelineOutput(newHashMap);
    return 0;
  }
}
