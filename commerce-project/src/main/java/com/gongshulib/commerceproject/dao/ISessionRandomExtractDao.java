package com.gongshulib.commerceproject.dao;

import com.gongshulib.commerceproject.domain.SessionRandomExtract;

/**
 * session随机抽取模块Dao接口
 * @author Administrator
 *
 */
public interface ISessionRandomExtractDao {
	
	/**
	 * 插入session随机抽取结果
	 * @param sessionRandomExtract
	 */
	void insert(SessionRandomExtract sessionRandomExtract);
}
