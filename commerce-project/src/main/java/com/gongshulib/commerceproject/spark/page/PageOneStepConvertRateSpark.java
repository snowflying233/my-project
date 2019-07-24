package com.gongshulib.commerceproject.spark.page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.gongshulib.commerceproject.constant.Constants;
import com.gongshulib.commerceproject.dao.IPageSplitConvertRateDao;
import com.gongshulib.commerceproject.dao.ITaskDao;
import com.gongshulib.commerceproject.dao.factory.DAOFactory;
import com.gongshulib.commerceproject.domain.PageSplitConvertRate;
import com.gongshulib.commerceproject.domain.Task;
import com.gongshulib.commerceproject.util.DateUtils;
import com.gongshulib.commerceproject.util.NumberUtils;
import com.gongshulib.commerceproject.util.ParamUtils;
import com.gongshulib.commerceproject.util.SparkUtils;

import scala.Tuple2;

/**
 * 页面单跳转化率模块
 * @author Administrator
 *
 */
public class PageOneStepConvertRateSpark {
	public static void main(String[] args) {
		//1.创建spark配置
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE);
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
		//2.生成模拟数据
		SparkUtils.mockData(sc, sqlContext);
		
		//3.查询任务，获取任务参数
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
		ITaskDao taskDao = DAOFactory.getTaskDao();
		Task task = taskDao.findById(taskid);
		
		if(task == null){
			System.out.println(new Date() + "cannot find this task with id +[" + taskid + "].");
			return;
		}

		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		//4.查询指定日期范围的用户行为数据
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		
		/**
		 * 该需求的核心
		 * 用户访问页面切片的生成
		 * 用使用者指定的页面流筛选生成的页面切片
		 */
		//先将用户行为数据进行映射<sessionid,Row>
		JavaPairRDD<String, Row> sessionid2rowRDD = getSessionid2rowRDD(actionRDD);
		sessionid2rowRDD = sessionid2rowRDD.persist(StorageLevel.MEMORY_ONLY());
		
		//访问页面的生成基于每一个session访问数据，脱离了session，生成的数据没有意义了
		//用户A访问了2，4|用户B访问了3，5看似页面流是2，3，4，5，但没有关系应为是两个用户
		JavaPairRDD<String,Iterable<Row>> sessionid2rowsRDD = sessionid2rowRDD.groupByKey();
		
		//用户访问页面切片生成以及根据使用者指定的页面流筛选算法
		JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(
				taskParam, sessionid2rowsRDD);
		
		//获取每个页面切片的点击量
		Map<String, Object> pageSplitPvRDD = pageSplitRDD.countByKey();
		
		//计算起始页面pv
		long startPagePv = getStartPagePv(taskParam, sessionid2rowsRDD);
		
		//计算页面切片转化率
		Map<String, Double> convertRateMap = computePageSplitConvertRate(
				taskParam, startPagePv, pageSplitPvRDD);
		
		//持久化页面转化率计算结果
		persistConvertRate(taskid, convertRateMap);
		
	}

	/**
	 * 对actionRDD进行映射
	 * 映射后数据格式(sessionid, row)
	 * @param actionRDD
	 * @return 
	 */
	private static JavaPairRDD<String, Row> getSessionid2rowRDD(
			JavaRDD<Row> actionRDD) {
		
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String sessionid = row.getString(2);
				return new Tuple2<String, Row>(sessionid, row);
			}
		});
	}
	
	/**
	 * 用户访问页面切片生成以及根据使用者指定的页面流筛选算法
	 * @param taskParam
	 * @param sessionid2rowsRDD
	 */
	private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
			JSONObject taskParam,
			JavaPairRDD<String, Iterable<Row>> sessionid2rowsRDD) {
		
		//获取使用者指定的页面流
		final String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		
		return sessionid2rowsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(
					Tuple2<String, Iterable<Row>> tuple) throws Exception {
				
				//存放返回结果
				List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
				
				//获取当前session访问行为数据迭代器
				Iterator<Row> iterator = tuple._2.iterator();
				
				String[] targetPages = targetPageFlow.split(",");
				
				//应为默认session访问行为数据是乱序的，我们先对其排序
				List<Row> rows = new ArrayList<Row>();
				while(iterator.hasNext()){
					rows.add(iterator.next());
				}
				
				Collections.sort(rows, new Comparator<Row>() {
					
					@Override
					public int compare(Row o1, Row o2) {
						String actionTime1 = o1.getString(4);
						String actionTime2 = o2.getString(4);
						
						Date date1 = DateUtils.parseTime(actionTime1);
						Date date2 = DateUtils.parseTime(actionTime2);
						
						return (int)(date1.getTime() - date2.getTime());
					}
					
				});
				
				//*页面切片的生成，以及页面流的匹配*
				Long lastPageId = null;
				
				for (Row row : rows) {
					long pageId = row.getLong(3);
					
					//第一次循环开始lastPageId为空，直接赋值，继续第二次循环
					if(lastPageId == null){
						lastPageId = pageId;
						continue;
					}
					
					// 生成一个页面切片
					// 3,5,2,1,8,9
					// lastPageId=3
					// 5，切片，3_5
					String pageSplit = lastPageId + "_" + pageId;
					
					//匹配，判断这个切片是否在用户指定的页面流中
					for(int i=1; i<targetPages.length; i++){
						// 比如说，用户指定的页面流是3,2,5,8,1
						// 遍历的时候，从索引1开始，就是从第二个页面开始
						// 3_2
						String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];
						
						//吻合说明符合要求，保存至List中返回
						if(pageSplit.equals(targetPageSplit)){
							list.add(new Tuple2<String, Integer>(pageSplit, 1));
							break;
						}
					}
					
					lastPageId = pageId;
				}
				
				return list;
			}
		});
	}
	
	/**
	 * 计算起始页面pv
	 * @param taskParam
	 * @param sessionid2rowsRDD
	 */
	private static long getStartPagePv(
			JSONObject taskParam, 
			JavaPairRDD<String, Iterable<Row>> sessionid2rowsRDD) {
		
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final long startPageid = Long.valueOf(targetPageFlow.split(",")[0]);
		
		JavaRDD<Long> startPageIdRDD = sessionid2rowsRDD.flatMap(
				new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Long> call(
					Tuple2<String, Iterable<Row>> tuple) throws Exception {
				//返回符合条件的值
				List<Long> list = new ArrayList<Long>();
				
				Iterator<Row> iterator = tuple._2.iterator();
				
				while(iterator.hasNext()){
					Row row = iterator.next();
					long pageid = row.getLong(3);
					
					if(pageid == startPageid){
						list.add(pageid);
					}
				}
				
				return list;
			}
		});
		
		return startPageIdRDD.count();
	}
	
	/**
	 * 计算页面切片转化率
	 * @param taskParam
	 * @param startPagePv
	 * @param pageSplitPvRDD
	 */
	private static Map<String, Double> computePageSplitConvertRate(
			JSONObject taskParam, 
			long startPagePv,
			Map<String, Object> pageSplitPvRDD) {

		//保存计算结果
		Map<String, Double> convertRateMap = new HashMap<String, Double>();
		
		String[] targetPageFlow = ParamUtils.getParam(
				taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
		
		// 3,5,2,4,6
		// 3_5
		// 3_5 pv / 3 pv
		// 5_2 rate = 5_2 pv / 3_5 pv
		
		long lastPageSplitPv = 0L;
		
		for(int i=1; i<targetPageFlow.length; i++){
			String pageSplit = targetPageFlow[i-1] + "_" + targetPageFlow[i];
			
			long pageSplitPv = Long.valueOf(String.valueOf(pageSplitPvRDD.get(pageSplit)));
			
			double convertRate = 0.0;
			
			if(i == 1){
				convertRate = NumberUtils.formatDouble(
						(double)pageSplitPv / (double)startPagePv, 2);
			}else {
				convertRate = NumberUtils.formatDouble(
						(double)pageSplitPv / (double)lastPageSplitPv, 2);
			}
			
			convertRateMap.put(pageSplit, convertRate);
			
			lastPageSplitPv = pageSplitPv;
		}
		
		return convertRateMap;
	}
	
	/**
	 * 页面切片转化率结果保存至MySQL中
	 * @param taskid
	 * @param convertRateMap
	 */
	private static void persistConvertRate(long taskid, 
			Map<String, Double> convertRateMap) {
		StringBuffer buffer = new StringBuffer("");
		
		for(Map.Entry<String, Double> convertRate : convertRateMap.entrySet()){
			String pageSplit = convertRate.getKey();
			double rate = convertRate.getValue();
			
			buffer.append(pageSplit + "=" + rate + "|");
		}
		
		String convertRateStr = buffer.toString();
		convertRateStr = convertRateStr.substring(0, convertRateStr.length()-1);
		
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskid(taskid);
		pageSplitConvertRate.setConvertRate(convertRateStr);
		
		IPageSplitConvertRateDao iPageSplitConvertRateDao = DAOFactory.getPageSplitConvertRateDao();
		iPageSplitConvertRateDao.insert(pageSplitConvertRate);
	}
}
