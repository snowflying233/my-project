package com.gongshulib.commerceproject.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;
import com.gongshulib.commerceproject.conf.ConfigurationManager;
import com.gongshulib.commerceproject.constant.Constants;
import com.gongshulib.commerceproject.test.MockData;

/**
 * Spark工具类
 * @author Administrator
 *
 */
public class SparkUtils {
	
	/**
	 * 判断是否是本地环境
	 * 设置setMaster()
	 * @param conf
	 */
	public static void setMaster(SparkConf conf){
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			conf.setMaster("local");
		}
	}
	
	/**
	 * 如果是本地环境，则创建SQLContext
	 * @param sc
	 * @return
	 */
	public static SQLContext getSQLContext(SparkContext sc){
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	/**
	 * 判断是否是本地环境，若为本地环境，则生成模拟数据。
	 */
	public static void mockData(JavaSparkContext sc, SQLContext sqlContext){
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			MockData.mock(sc, sqlContext);
		}
	}
	
	/**
	 * 获取指定日期范围内的用户访问行为数据
	 * @param sqlContext
	 * @param taskParam
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext, JSONObject taskParam){
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		String sql = 
				"select * "
				+ "from user_visit_action "
				+ "where date>='" + startDate + "' "
				+ "and date<='" + endDate +"'";
		DataFrame actionDF = sqlContext.sql(sql);
		
		//性能优化：调整SparkSQL 生成 RDD分区数量，优化sparkSQL低并行度性能问题
		//return actionDF.javaRDD().repartition(100);
		
		return actionDF.javaRDD();
	}
	
}
