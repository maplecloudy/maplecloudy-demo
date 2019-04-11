package com.maplecloudy.mapps;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.google.gson.Gson;
import com.maplecloudy.api.AppConstant;
import com.maplecloudy.api.app.AppPod;
import com.maplecloudy.app.MAppRunner;
import com.maplecloudy.app.MAppTool;
import com.maplecloudy.app.annotation.Action;
import com.maplecloudy.app.utils.MAppUtils;
import com.maplecloudy.security.FakeUnixGroupsMapping;

/**
 * @author fgzhong
 * @since 2019/3/31
 */
@Action
public class SparkSqlMain implements MAppTool {
  
  public int run(String[] args) throws Exception {
    AppPod appPod = MAppUtils.getAppPod();
//    String sql = (String) appPod.getConfigMap().get("sql");
//    String tableName = (String) appPod.getConfigMap().get("table");
//    String outPath = (String) appPod.getConfigMap().get("outPath");
    
    String sql = MAppUtils.getValue(appPod, "sql");
    String database = MAppUtils.getValue(appPod, "database");
    String outPath = MAppUtils.getValue(appPod, "outPath");
    String mAppId = System.getenv(AppConstant.MAPP_ID);
    String temporaryTableName = "tmp_" + mAppId;
    System.out.println(new Gson().toJson(appPod));
    System.out.println(sql);
    System.out.println(database);
    System.out.println(outPath);
    System.out.println(mAppId);
    System.out.println(temporaryTableName);
    
    MAppUtils.loadSparkConf();
    System.setProperty("spark.hadoop.hadoop.security.group.mapping",
        FakeUnixGroupsMapping.class.getName());
    System.out.println("**********spark conf 参数 pre*****************");
    System.out.println(new Gson().toJson(System.getProperties()));
    SparkConf scf = new SparkConf(true)
        .setAppName("maplecloudy-spark-hive-app-" + MAppUtils.getMAppId());
    MAppUtils.appendHadoopConf2Spark(scf);
    System.out.println("**********spark conf 参数 hadoop*****************");
    System.out.println(new Gson().toJson(System.getProperties()));
    MAppUtils.appendHiveConf2Spark(scf);
    System.out.println("**********spark conf 参数 hive*****************");
    System.out.println(new Gson().toJson(System.getProperties()));
    scf.set("spark.hadoop.hadoop.security.group.mapping",
        FakeUnixGroupsMapping.class.getName());
    System.out.println("====================================");
    System.out.println(scf.get("hive.metastore.warehouse.dir", "null-d"));
    System.out.println(new Gson().toJson(scf.getAll()));
    SparkSession spark = SparkSession.builder().config(scf).enableHiveSupport()
        .getOrCreate();
    SparkContext sparkContext = spark.sparkContext();
    JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);
    sc.hadoopConfiguration().addResource(MAppUtils.getHadoopConf());
    sc.hadoopConfiguration().addResource(MAppUtils.getHiveConf());
    spark.sql("use " + database);
    Dataset<Row> table = spark.sql(sql);
    table.write().format("parquet").mode(SaveMode.Overwrite)
        .option("path", outPath + "/" + mAppId).saveAsTable(temporaryTableName);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    System.exit(MAppRunner.run(new SparkSqlMain(), args));
  }
}
