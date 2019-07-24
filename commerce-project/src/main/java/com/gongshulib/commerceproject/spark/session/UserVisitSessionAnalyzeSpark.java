package com.gongshulib.commerceproject.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.gongshulib.commerceproject.conf.ConfigurationManager;
import com.gongshulib.commerceproject.constant.Constants;
import com.gongshulib.commerceproject.dao.ISessionAggrStatDao;
import com.gongshulib.commerceproject.dao.ISessionDetailDao;
import com.gongshulib.commerceproject.dao.ISessionRandomExtractDao;
import com.gongshulib.commerceproject.dao.ITaskDao;
import com.gongshulib.commerceproject.dao.ITop10CategoryDao;
import com.gongshulib.commerceproject.dao.ITop10SessionDao;
import com.gongshulib.commerceproject.dao.factory.DAOFactory;
import com.gongshulib.commerceproject.domain.SessionAggrStat;
import com.gongshulib.commerceproject.domain.SessionDetail;
import com.gongshulib.commerceproject.domain.SessionRandomExtract;
import com.gongshulib.commerceproject.domain.Task;
import com.gongshulib.commerceproject.domain.Top10Caregory;
import com.gongshulib.commerceproject.domain.Top10Session;
import com.gongshulib.commerceproject.test.MockData;
import com.gongshulib.commerceproject.util.DateUtils;
import com.gongshulib.commerceproject.util.NumberUtils;
import com.gongshulib.commerceproject.util.ParamUtils;
import com.gongshulib.commerceproject.util.SparkUtils;
import com.gongshulib.commerceproject.util.StringUtils;
import com.gongshulib.commerceproject.util.ValidUtils;
import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * 用户访问session分析
 * 接受用户创建的分析任务，用户可能指定的条件如下：
 * 1.时间范围：起始时间-结束时间
 * 2.性别：男或女
 * 3.年龄范围
 * 4.职业：多选
 * 5.城市：多选
 * 6.搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，session就符合条件
 * 7.点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，session就符合条件
 *
 * @author Administrator
 *
 */

@SuppressWarnings("unused")
public class UserVisitSessionAnalyzeSpark {
	public static void main(String[] args) {
		//args = new String[]{"1"};
		
		//创建Spark配置环境
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.set("spark.storage.memoryFraction", "0.5")
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64")
				.set("spark.shuffle.memoryFraction", "0.3");
		SparkUtils.setMaster(conf);
				
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());
		
		//生成模拟测试数据（数据在内存中）
		//mockData(sc, sqlContext);
		SparkUtils.mockData(sc, sqlContext);
		
		//创建需要使用的DAO组建
		ITaskDao taskDao = DAOFactory.getTaskDao();
		//首先查询出指定的任务，获取任务指定的参数
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		Task task = taskDao.findById(taskid);
		
		if(task == null){
			System.out.println(new Date() + "cannot find this task with id +[" + taskid + "].");
			return;
		}
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		/**
		 * 需求1：session粒度聚合
		 * 首先从user_visit_action表中，查询出制定日期范围内的行为数据
		 * 对actionRDD进行重构，getSessionid2ActionRDD(actionRDD)方法和aggregateBySession(sqlContext, actionRDD)方法
		 * 代码逻辑重复，为优化代码，进行重构
		 */
		
		//从user_visit_action表中，获取Task指定日期范围的数据
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		
		//对actionRDD的数据进行映射，便于后面进行表关联，数据格式：<sessionid, Row>
		JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
		
		//性能优化：常用到的RDD persist到内存中
		sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
		
		/**
		 * session聚合（session数据和用户数据）
		 * 最终数据格式：<sessionid, (sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
		 * 为提升性能
		 * 通过重构，把访问时长和访问步长也添加到session聚合的数据中去
		 * 最终数据合适：<sessionid, (sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime,age,professional,city,sex)>
		 */
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = 
				aggregateBySession(sc, sqlContext, sessionid2ActionRDD);
		
		/**
		 * 需求2：统计符合条件的session中，访问时长占比，访问步长占比
		 * 通过对session粒度的聚合数据进行过滤，使用自定义的Accumulate进行累计计算
		 */
		
		//自定义的Accumulate
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"", new SessionAggrStatAccumulator());
		
		//对session粒度聚合数据按照用户创建任务时指定条件进行过滤
		//对过滤后的数据进行访问时长，访问步长计算
		//重构，实现数据过滤和统计计算
		JavaPairRDD<String, String> filteredSession2AggrInfoRDD = filterSessionAndAggrStat(
				sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		
		//性能优化：将filteredSession2AggrInfoRDD持久化到内存中
		filteredSession2AggrInfoRDD = filteredSession2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		
		//生成公共RDD：通过筛选条件的session访问明细数据
		//sessionid2DetailRDD == Row
		JavaPairRDD<String, Row> sessionid2DetailRDD = 
				getSessionid2DetailRDD(filteredSession2AggrInfoRDD, sessionid2ActionRDD);
		
		sessionid2DetailRDD = sessionid2DetailRDD.persist(StorageLevel.MEMORY_ONLY());
		
		
		//定义action算子，触发spark作业
		System.out.println(filteredSession2AggrInfoRDD.count());
		
		//session聚合统计（统计访问时长和访问步长，各个区间的session数量占总session数量的比例）
		//结果保存至MySQL
		calculateAndPersistAggrSata(sessionAggrStatAccumulator.value(), 
				task.getTaskid());

		/**
		 * 需求3：按照时间比例随机抽取session
		 */
		randomExtractSession(sc, task.getTaskid(), filteredSession2AggrInfoRDD, 
				sessionid2DetailRDD);
		
		/**
		 * 需求4：top10热门品类
		 */
		List<Tuple2<CategorySortByKey, String>> top10CategoryList = 
				getTop10Category(task.getTaskid(), sessionid2DetailRDD);
		
		/**
		 * 需求5：获取top10活跃session
		 */
		getTop10Session(sc, task.getTaskid(), 
				top10CategoryList, sessionid2DetailRDD);
		
		//关闭Spark
		sc.close();
	}

	/**
	 * 获取SQLContext
	 * 如果是本地测试，生成SQLContext
	 * 如果是生产环境，生成HiveContext
	 * @param sc SparkContext
	 * @return SQLContext
	 */
	private static SQLContext getSQLContext(SparkContext sc){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	/**
	 * 生成模拟数据（只有本地模式，才会生成模拟数据）
	 * 包含两张表（user_visit_action和user_info）
	 * @param sc
	 * @param sqlContext
	 */
	private static void mockData(JavaSparkContext sc, SQLContext sqlContext){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			MockData.mock(sc, sqlContext);
		}
	}
	
	/**
	 * 从user_visit_action表中获取基础session数据
	 * 获取指定日期范围内的用户访问行为数据
	 * @param sqlContext
	 * @param taskParam
	 * @return 行为数据RDD
	 */
	private static JavaRDD<Row> getActionRDDByDateRange(
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
	
	/**
	 * 获取actionRDD 到<sessionid, Row>的映射
	 * 用来做关联数据明细
	 * @param actionRDD
	 */
	private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		
		/*
		return actionRDD.mapToPair(
			new PairFunction<Row, String, Row>() {
	
				private static final long serialVersionUID = 1L;
	
				@Override
				public Tuple2<String, Row> call(Row row) throws Exception {
					return new Tuple2<String, Row>(row.getString(2), row);
				}
			});
		*/
		
		//使用mapPartition算子代替map算子，适当提升性能
		return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
				
				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String,Row>>();
				
				while(iterator.hasNext()){
					Row row = iterator.next();
					
					list.add(new Tuple2<String, Row>(row.getString(2), row));
				}
					
				return list;
			}
		});
	}
	
	/**
	 * 对行为数据按session粒度进行聚合
	 * @param actionRDD
	 * @return sessionid2FullAggrInfoRDD
	 */
	private static JavaPairRDD<String, String> aggregateBySession(
			JavaSparkContext sc,
			SQLContext sqlContext, 
			JavaPairRDD<String, Row> sessionid2ActionRDDReplace){
		
		//重构后删除
		/*
		//将actionRDD映射成<sessionid,Row>格式
		JavaPairRDD<String, Row> sessionid2Action = actionRDD.mapToPair(
		new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			}
		});
		*/
		
		//对行为数据按照sessionid粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionRDD = 
				sessionid2ActionRDDReplace.groupByKey();
		
		//对按照sessionid粒度分组的数据进行处理
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionRDD.mapToPair(
				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {

						String sessionid = tuple._1;
						
						//Iterable<Row>可迭代的Row集合，调用iterator()方法返回Row集合迭代器
						Iterator<Row> iterator = tuple._2.iterator();
						
						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
						
						//获取session对应的userid，用于后面和userInfoRDD进行关联
						Long userid = null;
						
						//session的起始结束时间
						Date startTime = null;
						Date endTime = null;
						
						//session访问步长
						int stepLength = 0;
						
						//遍历每一个session对应的所有访问行为，将每一个session对应的所有的搜索词和点击品类聚合起来，并统计出访问时常，访问步长
						while(iterator.hasNext()){
							//使用next()方法即可获得迭代器中的 元素（Row）
							Row row = iterator.next();
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);
							
							if(userid == null){
								userid = row.getLong(1);
							}
							
							//searchKeyword不为空且searchKeywordsBuffer中不包含，那么就把这个searchKeyword 添加到searchKeywordsBuffer中
							if (StringUtils.isNotEmpty(searchKeyword)) {
								if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
									searchKeywordsBuffer.append(searchKeyword + ",");
								}
							}
							
							if (StringUtils.isNotEmpty(String.valueOf(clickCategoryId))){
								if (!clickCategoryIdsBuffer.toString().contains(
										String.valueOf(clickCategoryId))){
									clickCategoryIdsBuffer.append(clickCategoryId + ",");
								}
							}
							
							//取出session中的actionTime
							Date actionTime = DateUtils.parseTime(row.getString(4));
							
							//通过actionTime获取startTime和endTime
							//给startTime和endTime赋原始值
							if(startTime == null){
								startTime = actionTime;
							}
							if(endTime == null){
								endTime = actionTime;
							}
							
							//通过和获取的actionTime比较，更新startTime和endTime
							if(actionTime.before(startTime)){
								startTime = actionTime;
							}
							if(actionTime.after(endTime)){
								endTime = actionTime;
							}
							
							//计算session的访问步长
							//每一条记录相当于session的一次访问
							stepLength++;
							
						}
						
						//计算session访问时长
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						
						//提取searchKeywordsBuffer中的所有searchKeyword 并去掉起始或结尾的(,)逗号
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
								+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
								+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
						
						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
		});
		
		//查询所有用户数据，并映射成<userid,Row>的格式
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						return new Tuple2<Long, Row>(row.getLong(0), row);
					}
		});
		
		//将session粒度聚合数据，与用户信息进行join
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = 
				userid2PartAggrInfoRDD.join(userid2InfoRDD);
		
		//对join起来的数据进行拼接，返回<sessionid, fullAggrInfo>格式数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = 
				userid2FullInfoRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|" 
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex;
						
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
		});
		
		/**
		 * 数据倾斜解决方案(join)|代码调优：使用map join 代替 reduce join
		 */
		
		/*
		List<Tuple2<Long, Row>> userInfos = userid2InfoRDD.collect();
		final Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);
		
		JavaPairRDD<String, String> tunedRDD = userid2PartAggrInfoRDD.mapToPair(
				new PairFunction<Tuple2<Long,String>, String, String>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
						List<Tuple2<Long, Row>> userInfos = userInfosBroadcast.value();
						Map<Long, Row> userInfoMap = new HashMap<Long, Row>();
						
						for(Tuple2<Long, Row> userInfo : userInfos){
							userInfoMap.put(userInfo._1, userInfo._2);
						}
						
						String partAggrInfo = tuple._2;
						Row userInfoRow = userInfoMap.get(tuple._1);
						
						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|" 
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex;
						
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
				});
		*/
		
		/**
		 * 数据倾斜解决方案(join)：sample采样倾斜key进行两次join
		 */
		
		/*
		//随机抽取10%的数据
		JavaPairRDD<Long, String> sampledRDD = userid2PartAggrInfoRDD.sample(false, 0.1);
		
		//对抽取出的数据进行mapToPair映射
		JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(
				new PairFunction<Tuple2<Long,String>, Long, Long>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<Long, Long> call(Tuple2<Long, String> tuple) 
							throws Exception {
						return new Tuple2<Long, Long>(tuple._1, 1L);
					}
				});
		
		//对映射后的数据计算得出userid（key）对应出现的次数
		JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		
		//反转<userid,count>用于进行sortByKey
		JavaPairRDD<Long, Long> reversedSampledRDD = computedSampledRDD.mapToPair(
				new PairFunction<Tuple2<Long,Long>, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple) 
							throws Exception {
						return new Tuple2<Long, Long>(tuple._2, tuple._1);
					}
				});
		
		final long skewedUserid = reversedSampledRDD.sortByKey().take(1).get(0)._2;
		
		//将skewdUserid（倾斜的key）单独抽取出来形成一个RDD
		JavaPairRDD<Long, String> skewdRDD = userid2PartAggrInfoRDD.filter(
				new Function<Tuple2<Long,String>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
						return tuple._1.equals(skewedUserid);
					}
				});
		
		//没有倾斜的数据抽取出来形成另外一个RDD
		JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(
				new Function<Tuple2<Long,String>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
						return !tuple._1.equals(skewedUserid);
					}
				});
		
		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewdRDD.join(userid2InfoRDD);
		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRDD);
		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2);
		
		JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|" 
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex;
						
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
		});
		*/
		
		/**
		 * 数据倾斜方案（join）|使用随机数和扩容表解决join产生数据倾斜问题
		 */
		
		/*
		//将userid2InfoRDD进行扩容，扩大10倍
		JavaPairRDD<String, Row> expansionRDD =  userid2InfoRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String,Row>>();
						
						for(int i=0; i<10; i++){
							list.add(new Tuple2<String, Row>(i + "_" + tuple._1, tuple._2));
						}
						
						return list;
					}
				});
		
		//给userid2PartAggrInfoRDD的key打上10以内的随机值
		JavaPairRDD<String, String> mappedRDD = userid2PartAggrInfoRDD.mapToPair(
				new PairFunction<Tuple2<Long,String>, String, String>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
						Random random = new Random();
						int prefix = random.nextInt(10);
						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
					}
				});
		
		JavaPairRDD<String, Tuple2<String, Row>> joinedRDD = mappedRDD.join(expansionRDD);
		
		JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
				new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|" 
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex;
						
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
		});
		*/
		
		//最终数据格式：<sessionid, (sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
		//重构后最终数据格式：<sessionid, (sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
		return sessionid2FullAggrInfoRDD;
	}
	
	/**
	 * 通过参数过滤session数据，然后使用自定义的Accumulate计算访问时长，访问步长
	 * @param sessionid2AggrInfoRDD
	 * @param taskParam
	 * @param sessionAggrStatAccumulator
	 * @return 结果保存在自定义的Accumulate字符串中
	 */
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam, 
			final Accumulator<String> sessionAggrStatAccumulator){
		
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
		
		//如果parameter是以|结尾，就把|截去
		if(_parameter.endsWith("|")){
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		final String parameter = _parameter;
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfo = sessionid2AggrInfoRDD.filter(
				new Function<Tuple2<String,String>, Boolean>() {
					
					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						//从tuple中，获取聚合数据
						String aggrInfo = tuple._2;
						
						//按照筛选条件进行过滤
						//按照年龄范围进行筛选（startAge,endAge）
						/*
						int age = Integer.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_AGE));
						String startAge =ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
						String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
						
						if(startAge != null && endAge != null){
							
						}
						*/
						
						//按照筛选条件进行过滤
						//按照年龄范围进行筛选（startAge,endAge）
						if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
								parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
							return false;
						}
						
						//按照职业进行筛选（professionals）
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
								parameter, Constants.PARAM_PROFESSIONALS)){
							return false;
						}
						
						//按照城市范围进行筛选
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
								parameter, Constants.PARAM_CITIES)){
							return false;
						}
						
						//按照性别进行过滤
						if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
								parameter, Constants.PARAM_SEX)){
							return false;
						}
						
						//按照搜索词进行过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
								parameter, Constants.PARAM_KEYWORDS)){
							return false;
						}
						
						//按照点击品类进行过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
								parameter, Constants.PARAM_CATEGORY_IDS)){
							return false;
						}
						
						//程序走到这里，说明该条session符合用户的指定的筛选条件
						//进行访问时长和访问步长统计计算
						
						sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
						
						long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
						long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
						
						//计算访问时长和访问步长
						calculateVisitLength(visitLength);
						calculateStepLength(stepLength);
						
						return true;
					}
					
					//计算访问时长范围
					private void calculateVisitLength(long visitLength){
						if(visitLength >=1 && visitLength <=3){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
						} else if(visitLength >=4 && visitLength <=6){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
						} else if(visitLength >=7 && visitLength <=9){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
						} else if(visitLength >=10 && visitLength <=30){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
						} else if(visitLength >=30 && visitLength <=60){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
						} else if(visitLength >60 && visitLength <=180){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
						} else if(visitLength >180 && visitLength <=600){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
						} else if(visitLength >600 && visitLength <=1800){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
						} else if(visitLength >1800){
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
						}
					}
					
					//计算访问步长范围
					private void calculateStepLength(long stepLength){
						if(stepLength >=1 && stepLength <=3){
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
						} else if(stepLength >=4 && stepLength <=6){
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
						} else if(stepLength >=7 && stepLength <=9){
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
						} else if(stepLength >=10 && stepLength <=30){
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
						} else if(stepLength >30 && stepLength <=60){
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
						} else if(stepLength >60){
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
						}
					}
					
				});
		
		
		return filteredSessionid2AggrInfo;
	}
	
	/**
	 * 获取通过筛选条件的session访问明细数据RDD
	 * @param filteredSession2AggrInfoRDD
	 * @param sessionid2ActionRDD
	 * @return
	 */
	private static JavaPairRDD<String, Row> getSessionid2DetailRDD(
			JavaPairRDD<String, String> filteredSession2AggrInfoRDD, 
			JavaPairRDD<String, Row> sessionid2ActionRDD) {

		JavaPairRDD<String, Row> sessionid2DetailRDD = filteredSession2AggrInfoRDD
				.join(sessionid2ActionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
	
						private static final long serialVersionUID = 1L;
	
						@Override
						public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
							
							return new Tuple2<String, Row>(tuple._1, tuple._2._2);
						}
				});
		
		return sessionid2DetailRDD;
	}
	
	/**
	 * 过滤后的数据实现随机抽取
	 * @param sessionid2AggrInfoRDD
	 */
	private static void randomExtractSession(
			JavaSparkContext sc, 
			final long taskid, 
			JavaPairRDD<String, String> filteredSessionid2AggrInfo,
			JavaPairRDD<String, Row> sessionid2ActionRDD) {
		
		/**
		 * 第一步：计算出每天每小时的session数量
		 */
		
		//通过对过滤后的数据filteredSessionid2AggrInfo 进行mapToPair映射
		//<sessionid, (sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime,age,professional,city,sex)>
		//最终每条数据映射成如下格式：
		//time2SessionRDD: <yyyy-MM-dd_HH, aggrInfo>
		JavaPairRDD<String, String> time2SessionRDD = filteredSessionid2AggrInfo.mapToPair(
				new PairFunction<Tuple2<String,String>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
						String aggrInfo = tuple._2;
						
						String startTime = StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						
						//dateHour格式 yyyy-MM-dd_HH
						return new Tuple2<String, String>(dateHour, aggrInfo);
					}
		});
		
		//每天每小时对应的session数量
		//通过对time2SessionRDD: <yyyy-MM-dd_HH, aggrInfo> 格式的数据进行countByKey()
		//最终得到的就是每天每个小时对应的session数量（aggrInfo的数量）
		//countMap：<yyyy-MM-dd_HH, count>
		Map<String, Object> countMap = time2SessionRDD.countByKey();

		/**
		 * 第二步，使用按照时间比例随机抽取算法，计算出每天每小时要抽取session的索引
		 */
		
		//将countMap：<yyyy-MM-dd_HH, count> 格式的数据转换成 <yyyy-MM-dd, Map<HH, count>> 格式的数据
		//dayHourCountMap: <yyyy-MM-dd, Map<HH, count>>
		//hourCountMap: Map<String, Long>保存的是date对应的所有<hour, count>
		Map<String, Map<String, Long>> dateHourCountMap = 
				new HashMap<String, Map<String, Long>>();
		
		//遍历countMap 提取出date,hour,count
		for(Map.Entry<String, Object> entry: countMap.entrySet()){
			String dateHour = entry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			
			long count = Long.valueOf(String.valueOf(entry.getValue()));
			
			//获取date这天所对应的Map<String, Long>，如果不存在，就创建
			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if(hourCountMap == null){
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			
			//如果存在就向Map<String, Long>中继续put值
			hourCountMap.put(hour, count);
		}
		
		/*
		 * 实现按时间比例随机抽取算法
		 */
		
		//System.out.println("2019-04-24 Map<HH, count>: " + dateHourCountMap.get("2019-04-24").size());
		
		//总共要抽取100个session，抽取session总数/总天数=每天要抽取的session数量
		//每天需要抽取的session数量
		int extractNumberPerDay = 100 / dateHourCountMap.size(); 
		//System.out.println("抽取session总数/总天数|总天数：" + dateHourCountMap.size());
		
		//List中保存的是实现随机抽取数据的索引
		//dateHourExtractMap数据格式： <date, Map<hour, List<1,2,5,7>>>
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
				new HashMap<String, Map<String, List<Integer>>>();
		Random random = new Random();
		
		//遍历dateHourCountMap
		//提取出date,hour
		for (Map.Entry<String, Map<String, Long>>  dateHourCountEntry : dateHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			
			//hourCountMap: Map<String, Long>保存的是date对应的所有<hour, count>
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
			
			//计算出这一天的session总数
			long sessionCount = 0;
			for(long hourCount : hourCountMap.values()){
				sessionCount += hourCount;
			}
			
			//获取当前date对应的存放的hour和随机抽取数据的索引List
			//<hour, List<1,2,5>>
			//hourExtractMap: Map<String, List<Integer>>
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if(hourExtractMap == null){
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}
			
			for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()){
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				
				//（当前hour对应的count/当天session总数量）* 每天需要抽取的session数量 = 当前小时需要抽取的session数量
				//计算当前小时需要抽取session的数量
				int hourExtractNumber = (int)(((double)count / (double)sessionCount)
						* extractNumberPerDay);
				/*
				if(hourExtractNumber > count){
					hourExtractNumber = (int) count;
				}
				*/
				
				//获取当前小时所对应的存放随机数的List
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if(extractIndexList == null){
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				
				for(int i=0; i<hourExtractNumber; i++){
					int extractIndex = random.nextInt((int) count);
					//生成不重复的extractIndex
					while(extractIndexList.contains(extractIndex)){
						extractIndex = random.nextInt((int) count);
					}
					
					extractIndexList.add(extractIndex);
				}
			}
		}
		
		/**
		 * 添加广播变量
		 */
		final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractMapBroadCast = 
				sc.broadcast(dateHourExtractMap);
		
		/**
		 * 第三步：遍历每天每小时的session，然后根随机据索引进行抽取
		 */
		
		//对time2SessionRDD执行groupByKey() 算子 
		//返回<yyyy-MM-dd_HH, Iterable<aggrInfo>> 的数据
		JavaPairRDD<String, Iterable<String>> time2SessionsRDD = time2SessionRDD.groupByKey();
		
		//通过flatMapToPair()算子，遍历所有<dateHour, Iterable<aggrInfo>> 的数据
		//如果发现每个session数据恰巧和我们指定的随机抽取索引吻合
		//那么就抽取该session，直接写入Mysql的session_random_extract表中
		//用抽取出来的sessionid，去join它们的访问明细数据，数据写入到MySQL中session_detail表
		
		JavaPairRDD<String, String> extractSessionidsRDD = time2SessionsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, Iterable<String>> tuple) throws Exception {
						
						List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();
						
						String dateHour = tuple._1;
						String date = dateHour.split("_")[0]; 
						String hour = dateHour.split("_")[1]; 
						Iterator<String> iterator = tuple._2.iterator();
						
						/**
						 * 使用广播变量时，
						 * dateHourExtractMapBroadCast.value()|getValue()	
						 * 即可获得之前封装的广播变量
						 */
						Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
								dateHourExtractMapBroadCast.value();
						
						List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
						
						//System.out.println("size:" + extractIndexList.size());
						
						ISessionRandomExtractDao sessionRandomExtractDao = DAOFactory.getSessionRandomExtractDao();
						
						int index = 0;
						while(iterator.hasNext()){
							String sessionAggrInfo = iterator.next();
							
							//如果随机抽取算法List<Integer> extractIndexList集合中保存的索引包含当前索引
							//该条sessionAggrInfo数据符合条件，将数据信息保存到MySQL中
							//并将该条数据的sessionid保存到List<String> extractSessionids 集合中
							if(extractIndexList.contains(index)){
								String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
								
								//将数据写入MySQL
								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
								sessionRandomExtract.setTaskid(taskid);
								sessionRandomExtract.setSessionid(sessionid);
								sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
								sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
								sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
								
								sessionRandomExtractDao.insert(sessionRandomExtract);

								//将sessionid添加到extractIndexList中
								extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
							}
							
							index++;
						}
						
						//List<Tuple2<String, String>> extractSessionids
						//保存的是new Tuple2<String, String>(sessionid, sessionid)类型的数据
						//方便后面进行关联明细数据
						return extractSessionids;
					}
				});
		
		/**
		 * 第四步：获取明细数据
		 * extractSessionidsRDD.join(sessionid2ActionRDD)
		 * 通过sessionid关联明细数据
		 */
		
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = 
				extractSessionidsRDD.join(sessionid2ActionRDD);
		
		//遍历关联后的数据，然后插入到MySQL中
		extractSessionDetailRDD.foreach(
				new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
			
					private static final long serialVersionUID = 1L;
		
					@Override
					public void call(
							Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						Row row = tuple._2._2;
						
						SessionDetail sessionDetail = new SessionDetail();
						sessionDetail.setTaskid(taskid);
						sessionDetail.setUserid(row.getLong(1));
						sessionDetail.setSessionid(row.getString(2));
						sessionDetail.setPageid(row.getLong(3));
						sessionDetail.setActionTime(row.getString(4));
						sessionDetail.setSearchKeywords(row.getString(5));
						sessionDetail.setClickCategoryid(row.getLong(6));
						sessionDetail.setClickProductid(row.getLong(7));
						sessionDetail.setOrderCategoryIds(row.getString(8));
						sessionDetail.setOrderProductIds(row.getString(9));
						sessionDetail.setPayCategoryIds(row.getString(10));
						sessionDetail.setPayProductIds(row.getString(11));
						
						ISessionDetailDao sessionDetailDao = DAOFactory.getSessionDetailDao();
						sessionDetailDao.insert(sessionDetail);
					}
				});
		
		
	}
	
	/**
	 * 统计访问时长和访问步长占比，结果保存至MySQL
	 * @param value
	 * @param taskid
	 */
	private static void calculateAndPersistAggrSata(String value, long taskid) {
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		
		//计算访问时长和访问步长占比
		//访问时长占比
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);
		
		//访问步长占比
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);
		
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
		
		ISessionAggrStatDao iSessionAggrStatDao = DAOFactory.getSessionAggrStatDao();
		iSessionAggrStatDao.insert(sessionAggrStat);
	}
	
	/**
	 * 获取top10热门品类
	 * @param filteredSession2AggrInfoRDD <sessionid, (sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,age,professional,city,sex)>
	 * @param sessionid2ActionRDD <sessionid, Row>
	 */
	private static List<Tuple2<CategorySortByKey, String>> getTop10Category(
			long taskId,
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		
		/**
		 * 第一步：获取符合条件的session访问过的所有品类
		 */
		
		//获取session访问过的所有品类id
		//访问过指的是：点击过，下单过，支付过
		//最终数据格式：List<Tuple2<Long, Long>>|(categoryid, categoryid)
		 JavaPairRDD<Long, Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long,Long>>();
						
						Long clickCategoryId = row.getLong(6);
						if(clickCategoryId != null){
							list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
						}
						
						String orderCategoryIds = row.getString(8);
						if(orderCategoryIds != null){
							String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
							for(String orderCategoryId : orderCategoryIdsSplited){
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 
										Long.valueOf(orderCategoryId)));
							}
						}
						
						String payCategoryIds = row.getString(10);
						if(payCategoryIds != null){
							String[] payCategoryIdsSplited = payCategoryIds.split(",");
							for(String payCategoryId : payCategoryIdsSplited){
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 
										Long.valueOf(payCategoryId)));
							}
						}
						
						return list;
					}
				});
		 
		 /**
		  * 必须要进行去重
		  * 如果不去重，会出现重复的categoryid
		  * 最终导致错误的计算结果
		  */
		 categoryidRDD = categoryidRDD.distinct();

		/**
		 * 第二部：计算各个品类的点击，下单，支付次数
		 */
		
		//访问明细中，有三种访问行为：点击，下单和支付
		//分别计算各个品类的点击，下单和支付次数。先对访问明细数据进行过滤
		
		//计算各个品类的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = 
				getClickCategoryId2CountRDD(sessionid2DetailRDD);
		//计算各个品类的下单次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = 
				getOrderCategoryId2CountRDD(sessionid2DetailRDD);
		//计算各个品类的支付次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD =
				getPayCategoryId2CountRDD(sessionid2DetailRDD);
		 
		/**
		 * 第三步：join各个品类与它的点击，下单和支付的次数
		 * 
		 * join时需要注意使用leftOuterJoin
		 * 因为某个品类的商品可能只有点击，没有下单和支付次数
		 * 但是我们依然要用该品类品类，没有join到的数据，赋值为0即可
		 * 
		 */
		JavaPairRDD<Long, String> categoryid2CountRDD = joinCategoryAndData(
				categoryidRDD, 
				clickCategoryId2CountRDD, 
				orderCategoryId2CountRDD, 
				payCategoryId2CountRDD);
		
		/**
		 * 第四步：自定义二次排序
		 * CategorySortByKey.java
		 */
		
		/**
		 * 第五步：将数据映射成<CategorySortByKey,info>格式的RDD，然后进行二次排序（降序）
		 */
		JavaPairRDD<CategorySortByKey, String> sortByKey2CountRDD =  categoryid2CountRDD.mapToPair(
				new PairFunction<Tuple2<Long,String>, CategorySortByKey, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<CategorySortByKey, String> call(Tuple2<Long, String> tuple) throws Exception {
						String countInfo = tuple._2;
						
						long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
						long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
						long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_PAY_COUNT));
						
						CategorySortByKey sortByKey = new CategorySortByKey(clickCount, orderCount, payCount);
						
						return new Tuple2<CategorySortByKey, String>(sortByKey, countInfo);
					}
				});
		
		JavaPairRDD<CategorySortByKey, String> sortCategoryCountRDD = 
				sortByKey2CountRDD.sortByKey(false);
		
		/**
		 * 第六步：用take(10)取出top10热门品类，并写入Mysql
		 */
		List<Tuple2<CategorySortByKey, String>> top10CategoryList = 
				sortCategoryCountRDD.take(10);
		
		/**
		 * 遍历take(10)的数据，写入至MySQL
		 */
		ITop10CategoryDao iTop10CategoryDao = DAOFactory.getTop10CategoryDao();
		
		for(Tuple2<CategorySortByKey, String> tuple : top10CategoryList){
			String countInfo = tuple._2;
			
			long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_PAY_COUNT));
			
			Top10Caregory top10Caregory = new Top10Caregory();
			top10Caregory.setTaskId(taskId);
			top10Caregory.setCategoryId(categoryId);
			top10Caregory.setClickCount(clickCount);
			top10Caregory.setOrderCount(orderCount);
			top10Caregory.setPayCount(payCount);
			
			iTop10CategoryDao.insert(top10Caregory);
		}
		
		return top10CategoryList;
	}
	

	/**
	 * 获取各个品类的点击次数RDD
	 * @param sessionid2DetailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2DetailRDD) {

		//对sessionid对应的明细数据进行过滤，如果点击品类字段为null，则过滤掉
		JavaPairRDD<String, Row> clickActionRDD = sessionid2DetailRDD.filter(
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						
						return row.get(6) != null ? true : false;
					}
				});
		
		//将过滤后的数据映射成<clickCategoryId,1>类型
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
				new PairFunction<Tuple2<String,Row>, Long, Long>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
						long clickCategoryId = tuple._2.getLong(6);
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
				});
		
		/**
		 * 数据倾斜解决方案：随机key实现双重聚合
		 */
		
		/*
		//第一步：key值上面添加随机数，打散数据分布
		JavaPairRDD<String, Long> firstMappedClickCategoryIdRDD = clickCategoryIdRDD.mapToPair(
				new PairFunction<Tuple2<Long,Long>, String, Long>() {

					private static final long serialVersionUID = 1L;
		
					Random random = new Random();
					
					@Override
					public Tuple2<String, Long> call(Tuple2<Long, Long> tuple) throws Exception {
						int perfix = random.nextInt(10);
					
						return new Tuple2<String, Long>(perfix + "_" + tuple._1, tuple._2);
					}
				});
		
		//第二步：第一次聚合 
		JavaPairRDD<String, Long> firstAggrRDD = firstMappedClickCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		
		//第三步：去掉key值上面添加的随机数
		JavaPairRDD<Long, Long> secondMappedClickCategoryIdRDD = firstAggrRDD.mapToPair(
				new PairFunction<Tuple2<String,Long>, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Long> tuple) throws Exception {
						Long clickCategoryId = Long.valueOf(tuple._1.split("_")[1]);
						
						return new Tuple2<Long, Long>(clickCategoryId, tuple._2);
					}
				});
		
		//第四步：第二次聚合
		JavaPairRDD<Long, Long> globalAggrRDD = secondMappedClickCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		*/
		
		
		//对<clickCategoryId,1>类型的数据进行聚合，结果即为每个品类对应的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {
			
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		
		return clickCategoryId2CountRDD;
	}
	
	/**
	 * 获取各个品类的下单次数RDD
	 * @param sessionid2DetailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		
		JavaPairRDD<String, Row> orderActionRDD = sessionid2DetailRDD.filter(
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						
						return row.getString(8) != null ? true : false;
					}
				});
		
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long,Long>>();
						
						for(String orderCategoryId : orderCategoryIdsSplited){
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
						}
						
						return list;
					}
				});
		
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		
		return orderCategoryId2CountRDD;
	}
	
	/**
	 * 计算各个品类的支付次数RDD
	 * @param sessionid2DetailRDD
	 * @return payCategoryId2CountRDD
	 */
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		
		JavaPairRDD<String, Row> payActionRDD = sessionid2DetailRDD.filter(
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.getString(10) != null ? true : false;
					}
				});
		
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds.split(",");
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long,Long>>();
						
						for(String payCategoryId : payCategoryIdsSplited){
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
						}
						
						return list;
					}
				});
		
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		
		return payCategoryId2CountRDD;
	}
	
	/**
	 * categoryidRDD分别leftOuterJoin 点击，下单和支付RDD
	 * @param categoryidRDD
	 * @param clickCategoryId2CountRDD
	 * @param orderCategoryId2CountRDD
	 * @param payCategoryId2CountRDD
	 * @return
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndData(
			JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD, 
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = 
				categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
		
		JavaPairRDD<Long, String> tmpMapToPair =  tmpJoinRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) 
							throws Exception {

						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						long clickCount = 0L;
						
						if(optional.isPresent()){
							clickCount = optional.get();
						}
						
						String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + 
						Constants.FIELD_CLICK_COUNT + "=" + clickCount;
						
						return new Tuple2<Long, String>(categoryid, value);
					}
				});
		
		tmpMapToPair = tmpMapToPair.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long orderCount = 0L;
						
						if(optional.isPresent()){
							orderCount = optional.get();
						}
						
						value = value + "|" +  Constants.FIELD_ORDER_COUNT + "=" + orderCount;
						
						return new Tuple2<Long, String>(categoryid, value);
					}
		});
		
		tmpMapToPair = tmpMapToPair.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long payCount = 0L;
						
						if(optional.isPresent()){
							payCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
						
						return new Tuple2<Long, String>(categoryid, value);
					}
		});
		
		return tmpMapToPair;
	}
	
	/**
	 * 获取top10活跃session
	 * @param sc
	 * @param taskId
	 * @param top10CategoryList
	 * @param sessionid2DetailRDD
	 */
	private static void getTop10Session(
			JavaSparkContext sc, 
			final Long taskId,
			List<Tuple2<CategorySortByKey, String>> top10CategoryList, 
			JavaPairRDD<String, Row> sessionid2DetailRDD) {
		
		/**
		 * 第一步：将top10热门品类的id，生成一份RDD
		 */
		List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long,Long>>();
		
		for(Tuple2<CategorySortByKey, String> tuple : top10CategoryList){
			long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
					tuple._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
		}
		
		JavaPairRDD<Long, Long> top10CategoryIdRDD = 
				sc.parallelizePairs(top10CategoryIdList);
		
		/**
		 * 第二步：计算top10品类被各session点击次数
		 */
		
		//通过groupByKey() 获得每个sessionid对应的所有数据集合
		JavaPairRDD<String, Iterable<Row>> sessionIdDetailsRDD = 
				sessionid2DetailRDD.groupByKey();
		
		//categoryId2SessionCountRDD数据格式：<categoryId, "sessionId,count">
		JavaPairRDD<Long, String> categoryId2SessionCountRDD = sessionIdDetailsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Iterable<Tuple2<Long, String>> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionId = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						
						//计算出该session，对每个品类的点击次数
						while(iterator.hasNext()){
							Row row = iterator.next();
							
							if(row.get(6) != null){
								long categoryId = row.getLong(6);
								
								Long count = categoryCountMap.get(categoryId);
								if(count == null){
									count = 0L;
								}
								
								count++;
								
								categoryCountMap.put(categoryId, count);
							}
						}
						
						//返回结果，<categoryId, sessionId,count>
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long,String>>();
						
						for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()){
							long categoryId = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							
							String value = sessionId + "," + count;
							
							list.add(new Tuple2<Long, String>(categoryId, value));
						}
							
						return list;
					}
				});
		
		/*
		//遍历查看RDD结果
		categoryId2SessionCountRDD.foreach(new VoidFunction<Tuple2<Long,String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Lo
			ng, String> tuple) throws Exception {
				System.out.println("categoryId:"+ tuple._1 + "|sessionIdCount:" + tuple._2);
			}
		});
		*/
		
		//获取top10热门品类被各个session点击次数,数据类型：<categoryId, "sessionId,count">
		JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
			.join(categoryId2SessionCountRDD)
			.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {
	
				private static final long serialVersionUID = 1L;
	
				@Override
				public Tuple2<Long, String> call(
						Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
					return new Tuple2<Long, String>(tuple._1, tuple._2._2);
				}
			});
		
		/**
		 * 第三步：分组取topN算法实现，获取每个品类top10活跃用户
		 */
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = 
				top10CategorySessionCountRDD.groupByKey();
		
		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<Long, Iterable<String>> tuple) throws Exception {
						
						long categoryId = tuple._1;
						Iterator<String> iterator = tuple._2.iterator();
						
						//定义top10排序数组
						String[] top10Sessions = new String[10];
						
						//获取当前categoryId 对应的所有数据<categoryId, Iterable<String>>|String: "sessionid,clickCount"
						while(iterator.hasNext()){
							
							//遍历获取每一条数据
							String sessionCount = iterator.next();
							long count = Long.valueOf(sessionCount.split(",")[1]);
							
							//遍历top10数组进行排序
							for(int i=0; i<top10Sessions.length; i++){
								
								//如果获取到null,说明top10数组数据不足10条，直接将sessionCount放入数据，跳出循环
								if(top10Sessions[i] == null){
									top10Sessions[i] = sessionCount;
									break;
								}else {
									
									//如果取到了数据，那么提取出count数值进行比较
									long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
									
									//如果count的数值大于从top10数组中提取的_count数值，top10数组中的所有元素向后移一位
									if(count > _count){
										for(int j=9; j>i; j--){
											top10Sessions[j] = top10Sessions[j-1];
										}
										
										//top10数组中元素位置移动后，将[i]索引位置对应的值变为sessionCount，然后跳出循环
										top10Sessions[i] = sessionCount;
										break;
									}
									
									//走到这里说明count < _count，继续进行外层循环
								}
							}
						}
						
						//保存sessionid，join sessionid2DetailRDD获取session明细数据
						List<Tuple2<String, String>> sessionList = new ArrayList<Tuple2<String,String>>();
						
						//结果写入MySQL
						for(String sessionOrCount : top10Sessions){
							if(sessionOrCount != null){
								String sessionId = sessionOrCount.split(",")[0];
								long clickCount = Long.valueOf(sessionOrCount.split(",")[1]);
								
								Top10Session top10Session = new Top10Session();
								top10Session.setTaskId(taskId);
								top10Session.setCategoryId(categoryId);
								top10Session.setSessionId(sessionId);
								top10Session.setClickCount(clickCount);
								
								ITop10SessionDao top10SessionDao = DAOFactory.getTop10SessionDao();
								top10SessionDao.insert(top10Session);
							
								sessionList.add(new Tuple2<String, String>(sessionId, sessionId));
							}
						}
						
						return sessionList;
					}
				});
		
		/**
		 * 第四步：获取top10session的明细数据，保存至MySQL
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = 
				top10SessionRDD.join(sessionid2DetailRDD);
		
		//结果保存值MySQL
		sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public void call(
					Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
				
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskId);
				sessionDetail.setUserid(row.getLong(1));
				sessionDetail.setSessionid(row.getString(2));
				sessionDetail.setPageid(row.getLong(3));
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeywords(row.getString(5));
				sessionDetail.setClickCategoryid(row.getLong(6));
				sessionDetail.setClickProductid(row.getLong(7));
				sessionDetail.setOrderCategoryIds(row.getString(8));
				sessionDetail.setOrderProductIds(row.getString(9));
				sessionDetail.setPayCategoryIds(row.getString(10));
				sessionDetail.setPayProductIds(row.getString(11));
				
				ISessionDetailDao sessionDetailDao = DAOFactory.getSessionDetailDao();
				sessionDetailDao.insert(sessionDetail);
			}
		});

		
		
	}
	
	
}
