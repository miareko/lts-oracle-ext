package io.github.bitornot.lts.ext.monitor.access.oracle;

import io.github.bitornot.lts.ext.store.jdbc.builder.InsertSql;
import io.github.bitornot.lts.ext.utils.SqlUtils;

import java.util.List;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.monitor.access.domain.TaskTrackerMDataPo;
import com.github.ltsopensource.monitor.access.face.TaskTrackerMAccess;

/**
 * created by fanlu on 11/15/2016
 */
public class OracleTaskTrackerMAccess extends OracleAbstractAccess implements TaskTrackerMAccess {
	
	private static final String TABLE_NAME = "lts_admin_task_tracker_monitor";

	public OracleTaskTrackerMAccess(Config config) {
		super(config);
	}

	@Override
	public void insert(List<TaskTrackerMDataPo> taskTrackerMDataPos) {

        if (CollectionUtils.isEmpty(taskTrackerMDataPos)) {
            return;
        }

        InsertSql insertSql = new InsertSql(getSqlTemplate())
                .insert(getTableName())
                .columns(
                		"id",
                		"gmt_created",
                        "node_group",
                        "identity",
                        "timestamp_",
                        "exe_success_num",
                        "exe_failed_num",
                        "exe_later_num",
                        "exe_exception_num",
                        "total_running_time");

        for (TaskTrackerMDataPo taskTrackerMDataPo : taskTrackerMDataPos) {
            insertSql.values(
            		SqlUtils.getIdFromTimestamp(),
                    taskTrackerMDataPo.getGmtCreated(),
                    taskTrackerMDataPo.getNodeGroup(),
                    taskTrackerMDataPo.getIdentity(),
                    taskTrackerMDataPo.getTimestamp(),
                    taskTrackerMDataPo.getExeSuccessNum(),
                    taskTrackerMDataPo.getExeFailedNum(),
                    taskTrackerMDataPo.getExeLaterNum(),
                    taskTrackerMDataPo.getExeExceptionNum(),
                    taskTrackerMDataPo.getTotalRunningTime()
            );
        }

        insertSql.doBatchInsert();
	}

	@Override
	protected String getTableName() {
        return TABLE_NAME;
	}

}
