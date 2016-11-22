package io.github.bitornot.lts.ext.monitor.access.oracle;

import io.github.bitornot.lts.ext.store.jdbc.builder.InsertSql;
import io.github.bitornot.lts.ext.utils.SqlUtils;

import java.util.List;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.monitor.access.domain.JobTrackerMDataPo;
import com.github.ltsopensource.monitor.access.face.JobTrackerMAccess;

/**
 * created by fanlu on 11/15/2016
 */
public class OracleJobTrackerMAccess extends OracleAbstractAccess implements JobTrackerMAccess {

	private static final String TABLE_NAME = "lts_admin_job_tracker_monitor";
	
	public OracleJobTrackerMAccess(Config config) {
		super(config);
	}

	@Override
	public void insert(List<JobTrackerMDataPo> jobTrackerMDataPos) {

        InsertSql insertSql = new InsertSql(getSqlTemplate())
                .insert(getTableName())
                .columns(
                		"id",
                		"gmt_created",
                        "identity",
                        "timestamp_",
                        "receive_job_num",
                        "push_job_num",
                        "exe_success_num",
                        "exe_failed_num",
                        "exe_later_num",
                        "exe_exception_num",
                        "fix_executing_job_num");

        for (JobTrackerMDataPo jobTrackerMDataPo : jobTrackerMDataPos) {
            insertSql.values(
            		SqlUtils.getIdFromTimestamp(),
                    jobTrackerMDataPo.getGmtCreated(),
                    jobTrackerMDataPo.getIdentity(),
                    jobTrackerMDataPo.getTimestamp(),
                    jobTrackerMDataPo.getReceiveJobNum(),
                    jobTrackerMDataPo.getPushJobNum(),
                    jobTrackerMDataPo.getExeSuccessNum(),
                    jobTrackerMDataPo.getExeFailedNum(),
                    jobTrackerMDataPo.getExeLaterNum(),
                    jobTrackerMDataPo.getExeExceptionNum(),
                    jobTrackerMDataPo.getFixExecutingJobNum()
            );
        }
        insertSql.doBatchInsert();
	}

	@Override
	protected String getTableName() {
        return TABLE_NAME;
	}

}
