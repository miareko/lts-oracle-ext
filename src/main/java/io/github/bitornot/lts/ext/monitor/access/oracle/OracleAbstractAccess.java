package io.github.bitornot.lts.ext.monitor.access.oracle;

import io.github.bitornot.lts.ext.store.jdbc.oracle.OracleJdbcAbstractAccess;

import com.github.ltsopensource.core.cluster.Config;

/**
 * created by fanlu on 11/15/2016
 */
public abstract class OracleAbstractAccess extends OracleJdbcAbstractAccess {

	public OracleAbstractAccess(Config config) {
		super(config);
        createTable(readSqlFile("sql/oracle/" + getTableName() + ".sql"), getTableName());
	}

    protected abstract String getTableName();

}
