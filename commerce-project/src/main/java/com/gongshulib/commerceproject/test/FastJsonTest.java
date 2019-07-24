package com.gongshulib.commerceproject.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * FastJson测试类
 * @author Administrator
 *
 */
public class FastJsonTest {
	public static void main(String[] args) {
		
		//JSON格式的字符串
		String jsonString1 = "[{'name':'张三','age':18},{'name':'李四','age':20}]";
		String jsonString2 = "{'name':'张三','age':18}";
		
		//把JSON格式的字符串转化成JSONObject
		//JSONObject把元素映射成map
		JSONObject jsonObject1 = JSONObject.parseObject(jsonString2);
		System.out.println(jsonObject1.getString("age"));
		
		JSONArray jsonArray = JSONArray.parseArray(jsonString1);
		JSONObject jsonObject2 = jsonArray.getJSONObject(1);
		
		System.out.println(jsonObject2.getString("name"));
	}
	
}
