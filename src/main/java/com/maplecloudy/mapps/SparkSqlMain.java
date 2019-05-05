package com.maplecloudy.mapps;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.google.gson.Gson;
import com.maplecloudy.api.AppConstant;
import com.maplecloudy.app.MAppRunner;
import com.maplecloudy.app.MAppTool;
import com.maplecloudy.app.annotation.Action;
import com.maplecloudy.app.utils.MAppUtils;
import com.maplecloudy.hadoop.secutiry.FakeUnixGroupsMapping;

import jersey.repackaged.com.google.common.collect.Maps;

/**
 * @author fgzhong
 * @since 2019/3/31
 */
@Action
public class SparkSqlMain implements MAppTool {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  public int run(String[] args) throws Exception {
    
    String sql = MAppUtils.getParameter("sql", "");
    String database = MAppUtils.getParameter("database", "default");
    String outPath = MAppUtils.getParameter("outPath", "");
    String resultNum = MAppUtils.getParameter("resultNum", "1");
    String mAppId = System.getenv(AppConstant.MAPP_ID);
    String tmpDB = MAppUtils.getParameter(AppConstant.TMP_MAPP_DB,
        AppConstant.TMP_MAPP_DB_NAME);
    String temporaryTableName = "tmp_" + mAppId;
    Configuration hiveConf = MAppUtils.getHiveConf();
    FileSystem fs = FileSystem.get(hiveConf);
    // 取Path的绝对路径
    outPath = new Path(fs.getHomeDirectory(), outPath).toString();
    
    System.out.println(sql);
    System.out.println(database);
    System.out.println(outPath);
    System.out.println(mAppId);
    System.out.println(temporaryTableName);
    
    MAppUtils.loadSparkConf();
    
    SparkConf scf = new SparkConf(true)
        .setAppName("maplecloudy-spark-hive-app-" + MAppUtils.getMAppId());
    
    hiveConf.set("hive.exec.local.scratchdir", System.getProperty("user.dir")
        + File.separator + System.getenv("USER"));
    hiveConf.set("hive.server2.thrift.client.user", System.getenv("USER"));
    hiveConf.set("hive.server2.thrift.client.password", "");
    hiveConf.set("hadoop.security.group.mapping",
        FakeUnixGroupsMapping.class.getName());
    MAppUtils.appendHiveConf2Spark(scf);
    
    System.out.println(new Gson().toJson(scf.getAll()));
    SparkSession spark = SparkSession.builder().config(scf).enableHiveSupport()
        .getOrCreate();
    SparkContext sparkContext = spark.sparkContext();

    MAppUtils.saveSparkContext(sparkContext);
    Configuration conf = SparkHadoopUtil.get().conf();
    Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

    JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);
    sc.hadoopConfiguration().addResource(MAppUtils.getHadoopConf());
    sc.hadoopConfiguration().addResource(MAppUtils.getHiveConf());
    System.out.println(new Gson().toJson(scf.getAll()));
    spark.sql("use " + database);
    Dataset<Row> table = spark.sql(sql);
    spark.sql("use " + tmpDB);
    table.coalesce(Integer.valueOf(resultNum)).write().format("parquet")
        .mode(SaveMode.Overwrite).option("path", outPath + "/" + mAppId)
        .saveAsTable(temporaryTableName);
    long rowNum = table.count();
    HashMap<String,Object> output = Maps.newHashMap();
    output.put("rowNum", rowNum);
    MAppUtils.savePipelineOutput(output);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    System.exit(MAppRunner.run(new SparkSqlMain(), args));
  }
}
