package com.gongshulib.commerceproject.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gongshulib.commerceproject.conf.ConfigurationManager;
import com.gongshulib.commerceproject.constant.Constants;

/**
 * 参数工具类
 * @author Administrator
 *
 */
public class ParamUtils {


	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	/*
	public static Long getTaskIdFromArgs(String[] args, String taskType) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local) {
			return ConfigurationManager.getLong(taskType);  
		} else {
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}

	 */
	
	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args, String taskType) {
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local){
			return ConfigurationManager.getLong(taskType);
		}else {
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		 
		return null;
	}
	
	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject JSON对象
	 * @return 参数
	 */
	public static String getParam(JSONObject jsonObject, String field) {
		//获取指定字段所对应的值，这个值可能是一个数组
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		if(jsonArray != null && jsonArray.size() > 0) {
			//不管数组中包含多少元素只返回数组中第一个元素
			return jsonArray.getString(0);
		}
		
		/*
		if(jsonObject.getString(field) != null){
			return jsonObject.getString(field);
		}
		*/
		
		return null;
	}
	
}
