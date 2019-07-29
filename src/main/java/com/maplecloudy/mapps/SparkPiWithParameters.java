package com.maplecloudy.mapps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
public class SparkPiWithParameters implements MAppTool {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  public static void main(String[] args) throws Exception {
    System.exit(MAppRunner.run(new SparkPiWithParameters(), args));
  }
  
  @Override
  public int run(String[] args) throws Exception {
    // 参数检查  如果输入slices 否则切割二次
    System.out
        .println("输入的slices参数为（默认是2）：" + MAppUtils.getParameter("slices", "2"));
    System.out.println(
        "输入的times参数为（默认是100000）：" + MAppUtils.getParameter("times", "100000"));
    int slices = Integer.valueOf(MAppUtils.getParameter("slices", "2"));
    int n = Integer.valueOf(MAppUtils.getParameter("times", "100000")) * slices;
    List<Integer> l = new ArrayList<Integer>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }
    
    System.out
        .println("******************开始加载spark参数，使用用户设置的参数进行覆盖**************");
    MAppUtils.loadSparkConf();
    SparkConf sparkConf = new SparkConf(true).setAppName("JavaSparkPi");
    MAppUtils.appendHiveConf2Spark(sparkConf);
    System.out.println(
        "******************打印spark所有参数内容*********************************");
    for (Tuple2<String,String> tp : sparkConf.getAll()) {
      System.out.println(tp._1 + ":" + tp._2);
    }
    System.out.println(
        "******************打印所有系统环境变量参数*********************************");
    Map<String,String> getenv = System.getenv();
    for (Map.Entry<String,String> env : getenv.entrySet()) {
      System.out.println(
          "System env: key=" + env.getKey() + ", val=" + env.getValue());
    }
    
    System.out.println(
        "******************获得用户在平台设置的所有参数信息*********************************");
    AppPod appPod = MAppUtils.getAppPod();
    System.out.println("spark pi输出apppod信息");
    if (appPod != null) {
      System.out
          .println("appPod信息：" + AppConstant.om.writeValueAsString(appPod));
    } else {
      System.out.println("appPod is null");
    }
    System.out.println(
        "******************开始加载用户在平台上设置的参数之Parameters******************************");
    HashMap<String,Object> parameter = MAppUtils.getAppPod().getParameter();
    System.out.println("全部输入参数：" + new Gson().toJson(parameter));
    
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    Configuration conf = jsc.hadoopConfiguration();
    
    System.out.println(
        "******************开始输出所有Hadooop配置参数******************************");
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
    MAppUtils.savePipelineOutput("piResult" + 4.0 * count / n);
    return 0;
  }
}
