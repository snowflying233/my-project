package com.gongshulib.commerceproject;

/*
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
*/


/**
 * Hello world!
 *
 */
public class App 
{
	public static void main( String[] args )
	{
		//System.out.println( "Hello World!" );
		
		//测试JSONObject
		/*
		System.out.println("test JSONObject!");
		String _param = "{'startAge':['10'],'endAge':['50'],'startDate':['2019-07-09','2019-07-10'],'endDate':['2019-07-09']}";
		
		JSONObject param = JSONObject.parseObject(_param);
		System.out.println(param.get("startDate"));
		System.out.println(ParamUtils.getParam(param, "startAge"));
		*/
		
		//测试flatMap
		/*
		SparkConf conf = new SparkConf()
				.setAppName("App")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		ArrayList<Integer> arr1 = new ArrayList<Integer>();
		arr1.add(10);
		arr1.add(20);
		arr1.add(30);
		
		ArrayList<Integer> arr2 = new ArrayList<Integer>();
		arr2.add(40);
		arr2.add(50);
		arr2.add(60);
		
		Tuple2<String, List<Integer>> tuple1 = new Tuple2<String, List<Integer>>("zhangsan", arr1);
		Tuple2<String, List<Integer>> tuple2 = new Tuple2<String, List<Integer>>("lisi", arr2);
		
		List<Tuple2<String, List<Integer>>> list = new ArrayList<Tuple2<String,List<Integer>>>();
		list.add(tuple1);
		list.add(tuple2);
		
		JavaPairRDD<String, List<Integer>> string2integerRDD = sc.parallelizePairs(list);
		
		JavaPairRDD<Integer, Integer> integer2integerRDD = string2integerRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,List<Integer>>, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Integer, Integer>> call(
					Tuple2<String, List<Integer>> tuple) throws Exception {
				
				List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
				
				Iterator<Integer> iterator = tuple._2.iterator();
				
				while(iterator.hasNext()){
					int num = iterator.next();
					list.add(new Tuple2<Integer, Integer>(num, 1));
				}
				
				return list;
			}
		});
		
		integer2integerRDD.foreach(new VoidFunction<Tuple2<Integer,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Integer> tuple) throws Exception {
				System.out.println("tuple._1:" + tuple._1 + " tuple._2:" + tuple._2);
			}
		});
		
		
		
		
		sc.close();
		*/
		
		
	}
}
