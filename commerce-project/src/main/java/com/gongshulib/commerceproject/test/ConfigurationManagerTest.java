package com.gongshulib.commerceproject.test;

import com.gongshulib.commerceproject.conf.ConfigurationManager;

/**
 * 配置管理组件测试类
 * @author Administrator
 *
 */
public class ConfigurationManagerTest {
	public static void main(String[] args) {
		
		String testkey1 = ConfigurationManager.getProperty("jdbc.user");
		String testkey2 = ConfigurationManager.getProperty("testkey2");
		
		System.out.println("testkey1:" + testkey1);
		System.out.println("testkey2:" + testkey2);
	}
	
	
}
