package io.github.bitornot.lts.ext.store.jdbc.oracle;

import io.github.bitornot.lts.ext.store.jdbc.builder.SelectSql;

import java.math.BigDecimal;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.store.jdbc.JdbcAbstractAccess;
import com.github.ltsopensource.store.jdbc.exception.JdbcException;

/**
 * created by fanlu on 11/15/2016
 */
public abstract class OracleJdbcAbstractAccess extends JdbcAbstractAccess {

	public OracleJdbcAbstractAccess(Config config) {
		super(config);
	}
	
	protected void createTable(String sql, String tableName) throws JdbcException {
    	
    	int num = ((BigDecimal) new SelectSql(getSqlTemplate())
    							.select()
    							.columns(getSqlCheckTableExists(tableName))
    							.from()
    							.table("DUAL")
    							.single()).intValue();
    	
    	if (num > 0) {
    		return;
    	}
        
    	try {
            getSqlTemplate().createTable(sql);
        } catch (Exception e) {
            throw new JdbcException("Create table error, sql=" + sql, e);
        }
    }
	
	private String getSqlCheckTableExists(String tableName) {
		return new StringBuilder().append("FUNC_IFTABLEEXISTS('").append(tableName).append("')").toString();
	}

}
