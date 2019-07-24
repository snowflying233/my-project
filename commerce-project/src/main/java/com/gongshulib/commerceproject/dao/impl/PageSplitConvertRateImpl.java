package com.gongshulib.commerceproject.dao.impl;

import com.gongshulib.commerceproject.dao.IPageSplitConvertRateDao;
import com.gongshulib.commerceproject.domain.PageSplitConvertRate;
import com.gongshulib.commerceproject.jdbc.JDBCHelper;

public class PageSplitConvertRateImpl implements IPageSplitConvertRateDao{

	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";
		Object[] params = new Object[]{
				pageSplitConvertRate.getTaskid(), 
				pageSplitConvertRate.getConvertRate()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
