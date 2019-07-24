package com.gongshulib.commerceproject.dao.impl;

import com.gongshulib.commerceproject.dao.ISessionRandomExtractDao;
import com.gongshulib.commerceproject.domain.SessionRandomExtract;
import com.gongshulib.commerceproject.jdbc.JDBCHelper;

public class SessionRandomExtractDaoImpl implements ISessionRandomExtractDao{

	/**
	 * 插入session随机抽取
	 */
	@Override
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		Object[] params = new Object[]{sessionRandomExtract.getTaskid()
				,sessionRandomExtract.getSessionid()
				,sessionRandomExtract.getStartTime()
				,sessionRandomExtract.getSearchKeywords()
				,sessionRandomExtract.getClickCategoryIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
