package com.gongshulib.commerceproject.dao;

import com.gongshulib.commerceproject.domain.SessionDetail;

/**
 * session明细Dao
 * @author Administrator
 *
 */
public interface ISessionDetailDao {

	/**
	 * 插入session明细数据
	 * @param sessionDetail
	 */
	void insert(SessionDetail sessionDetail);
}
