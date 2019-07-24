package com.gongshulib.commerceproject.dao.impl;

import com.gongshulib.commerceproject.dao.ISessionDetailDao;
import com.gongshulib.commerceproject.domain.SessionDetail;
import com.gongshulib.commerceproject.jdbc.JDBCHelper;

public class SessionDetailDaoImpl implements ISessionDetailDao{

	/**
	 * 插入session明细数据实现
	 */
	@Override
	public void insert(SessionDetail sessionDetail) {
		
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[]{sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeywords(),
				sessionDetail.getClickCategoryid(),
				sessionDetail.getClickProductid(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}
	
	
}
