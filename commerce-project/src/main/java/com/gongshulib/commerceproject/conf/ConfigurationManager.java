package com.gongshulib.commerceproject.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 用来加载my.properties文件中配置的内容
 * @author Administrator
 *
 */
public class ConfigurationManager {
	private static Properties prop = new Properties();
	
	/**
	 * 静态代码块中，类的初始化在JVM生命周期中有且仅有一次。
	 * 也就是配置文件只会被加载一次，后面就是重复使用。效率较高，不用反复加载多次。
	 */
	static{
		try {
			//类名.class 可以获取JVM中对应的class对象
			//getClassLoader()方法可以获取当初加载这个类的类加载器（ClassLoader）
			//getResourceAsStream加载配置文件，形成输入流（InputStream）
			InputStream in = ConfigurationManager.class
						.getClassLoader().getResourceAsStream("my.properties");
			
			//调用Properties的load方法，传入一个文件的输入流（InputStream）
			//就可以把文件中KEY-VALUE格式的配置项，加载到Properties对象中
			//然后就可以使用Properties来获取指定key对应的value值
			prop.load(in);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取指定key所对应的value
	 * @param key
	 * @return
	 */
	public static String getProperty(String key){
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return
	 */
	public static Integer getInteger(String key) {
		String value = prop.getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取boolean类型配置项
	 * @param key
	 * @return
	 */
	public static Boolean getBoolean(String key){
		String value = prop.getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key){
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0L;
	}
	
}
