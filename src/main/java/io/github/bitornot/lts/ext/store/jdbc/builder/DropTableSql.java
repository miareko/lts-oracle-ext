package io.github.bitornot.lts.ext.store.jdbc.builder;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.store.jdbc.SQLFormatter;
import com.github.ltsopensource.store.jdbc.SqlTemplate;
import com.github.ltsopensource.store.jdbc.exception.JdbcException;

/**
 * created by fanlu on 11/15/2016
 */
public class DropTableSql {


    private static final Logger LOGGER = LoggerFactory.getLogger(DropTableSql.class);

    private SqlTemplate sqlTemplate;
    private StringBuilder sql = new StringBuilder();

    public DropTableSql(SqlTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
    }

    public DropTableSql drop(String table) {
    	sql.append("CALL PROC_DROPIFEXISTS('").append(table).append("')");
//		sql.append("DROP TABLE ").append(table);
        return this;
    }

    public boolean doDrop() {

        String finalSQL = sql.toString();

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(SQLFormatter.format(finalSQL));
            }

            sqlTemplate.update(sql.toString());
        } catch (Exception e) {
            throw new JdbcException("Drop Table Error:" + SQLFormatter.format(finalSQL), e);
        }
        return true;
    }

}
