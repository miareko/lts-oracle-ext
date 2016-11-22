package io.github.bitornot.lts.ext.monitor.access.oracle;

import io.github.bitornot.lts.ext.store.jdbc.builder.InsertSql;
import io.github.bitornot.lts.ext.utils.SqlUtils;

import java.util.List;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.monitor.access.domain.JVMThreadDataPo;
import com.github.ltsopensource.monitor.access.face.JVMThreadAccess;

/**
 * created by fanlu on 11/15/2016
 */
public class OracleJVMThreadAccess extends OracleAbstractAccess implements JVMThreadAccess {

	private static final String TABLE_NAME = "lts_admin_jvm_thread";
	
	public OracleJVMThreadAccess(Config config) {
		super(config);
	}

	@Override
	public void insert(List<JVMThreadDataPo> jvmThreadDataPos) {
        if (CollectionUtils.isEmpty(jvmThreadDataPos)) {
            return;
        }

        InsertSql insertSql = new InsertSql(getSqlTemplate())
                .insert(getTableName())
                .columns(
                		"id",
                		"gmt_created",
                        "identity",
                        "timestamp_",
                        "node_type",
                        "node_group",
                        "daemon_thread_count",
                        "thread_count",
                        "total_started_thread_count",
                        "dead_locked_thread_count",
                        "process_cpu_time_rate");

        for (JVMThreadDataPo po : jvmThreadDataPos) {
            insertSql.values(
            		SqlUtils.getIdFromTimestamp(),
                    po.getGmtCreated(),
                    po.getIdentity(),
                    po.getTimestamp(),
                    po.getNodeType().name(),
                    po.getNodeGroup(),
                    po.getDaemonThreadCount(),
                    po.getThreadCount(),
                    po.getTotalStartedThreadCount(),
                    po.getDeadLockedThreadCount(),
                    po.getProcessCpuTimeRate()
            );
        }

        insertSql.doBatchInsert();
	}

	@Override
	protected String getTableName() {
        return TABLE_NAME;
	}

}
