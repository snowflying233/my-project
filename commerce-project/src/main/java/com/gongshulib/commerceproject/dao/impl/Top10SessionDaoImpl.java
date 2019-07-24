package com.gongshulib.commerceproject.dao.impl;

import com.gongshulib.commerceproject.dao.ITop10SessionDao;
import com.gongshulib.commerceproject.domain.Top10Session;
import com.gongshulib.commerceproject.jdbc.JDBCHelper;

/**
 * ITop10SessionDao的实现
 * @author Administrator
 *
 */
public class Top10SessionDaoImpl implements ITop10SessionDao{

	@Override
	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_session values(?,?,?,?)";
		Object[] params = new Object[]{top10Session.getTaskId(),
				top10Session.getCategoryId(),
				top10Session.getSessionId(),
				top10Session.getClickCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
