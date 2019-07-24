package com.gongshulib.commerceproject.dao.impl;

import com.gongshulib.commerceproject.dao.ITop10CategoryDao;
import com.gongshulib.commerceproject.domain.Top10Caregory;
import com.gongshulib.commerceproject.jdbc.JDBCHelper;

public class Top10CategoryDaoImpl implements ITop10CategoryDao{

	@Override
	public void insert(Top10Caregory top10Caregory) {
		
		String sql = "insert into top10_category values(?,?,?,?,?)";
		Object[] params = new Object[]{top10Caregory.getTaskId(),
				top10Caregory.getCategoryId(),
				top10Caregory.getClickCount(),
				top10Caregory.getOrderCount(),
				top10Caregory.getPayCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
