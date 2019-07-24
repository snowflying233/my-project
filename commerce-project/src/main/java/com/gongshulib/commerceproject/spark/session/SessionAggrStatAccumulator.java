package com.gongshulib.commerceproject.spark.session;

import org.apache.spark.AccumulatorParam;

import com.gongshulib.commerceproject.constant.Constants;
import com.gongshulib.commerceproject.util.StringUtils;

/**
 * 自定义Accumulator
 * session聚合统计Accumulator
 * @author Administrator
 *
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String>{
	
	private static final long serialVersionUID = 8227783505264201020L;

	/**
	 * zero方法，主要用于数据的初始化
	 * 返回的这个值，就是初始化，所有区间范围所对应的值，都是0
	 * 各个范围区间的统计数量的拼接，还是采用一如既往的key=value|key=value的连接串的格式
	 * sc.accumulator("", new SessionAggrStatAccumulator())
	 * 这里v的值：就是""(String initialValue="")
	 */
	@Override
	public String zero(String v) {
		return Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}
	
	/**
	 * 最终返回sessionAggrStatAccumulator对象的值
	 * 这里v的值：就是""(String initialValue="")
	 * v2	我们自定义的字符串处理后的结果
	 * v2:	session_count=624|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=4|3m_10m=7|10m_30m=42|30m=551|1_3=74|4_6=94|7_9=101|10_30=352|30_60=3|60=0
	 */
	@Override
	public String addInPlace(String v, String v2) {
		return add(v, v2);
	}

	/**
	 * v1的值就是初始化zero(String v)方法后返回的值（我们自定义的字符串）
	 * v2是我们传进来的参数也就是范围区间
	 * 所要做的就是在v1中找到v2所对应value，累加1，然后再更新回连接串中去
	 */
	@Override
	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}
	
	/**
	 * session统计计算逻辑
	 * @param v1 连接串
	 * @param v2 范围区间 
	 * @return 更新后的连接串
	 */
	private String add(String v1, String v2){
		
		/**
		 * if()方法
		 * addInPlace(String v, String v2)调用add()方法
		 * 校检：v1为空的话，直接返回v2
		 * 返回最终sessionAggrStatAccumulator对象的值
		 */
		if(StringUtils.isEmpty(v1)){
			return v2;
		}
		
		//使用StringUtils工具类，从v1中获取v2字段所对应的value，如果不为null，则累加1
		String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		if(oldValue != null){
			int newValue = Integer.valueOf(oldValue) + 1;
			
			//累加完毕后，更新v1字段
			//即把newValue的值再设置到v1中
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
		
		//oldValue为null的情况，可能是v2字段不合法
		return v1;
	}
}
