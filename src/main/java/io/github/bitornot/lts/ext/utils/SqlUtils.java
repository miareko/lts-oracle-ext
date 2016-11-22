package io.github.bitornot.lts.ext.utils;


/**
 * created by fanlu on 11/15/2016
 */
public class SqlUtils {
	
	public synchronized static long getIdFromTimestamp() {
		long id = System.currentTimeMillis();
		
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return id;
	}
	
	public static java.sql.Date getSqlDate(java.util.Date date) {
		
		return date != null ? new java.sql.Date(date.getTime()) : null; 
	}
	
}
