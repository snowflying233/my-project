package com.gongshulib.commerceproject.dao;

import com.gongshulib.commerceproject.domain.SessionAggrStat;

/**
 * session聚合统计模块Dao接口
 * @author Administrator
 *
 */
public interface ISessionAggrStatDao {
	
	/**
	 * session聚合统计结果保存至mysql
	 * @param sessionAggrStat
	 */
	void insert(SessionAggrStat sessionAggrStat);
}
