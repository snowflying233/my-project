package com.gongshulib.commerceproject.dao;

import com.gongshulib.commerceproject.domain.Top10Session;

public interface ITop10SessionDao {
	
	/**
	 * 向MySQL数据库中插入数据
	 * @param top10Session
	 */
	void insert(Top10Session top10Session);
}
