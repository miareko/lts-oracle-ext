package io.github.bitornot.lts.ext.monitor.access.oracle;

import io.github.bitornot.lts.ext.store.jdbc.builder.InsertSql;
import io.github.bitornot.lts.ext.utils.SqlUtils;

import java.util.List;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.monitor.access.domain.JobClientMDataPo;
import com.github.ltsopensource.monitor.access.face.JobClientMAccess;

/**
 * created by fanlu on 11/15/2016
 */
public class OracleJobClientMAcess extends OracleAbstractAccess implements JobClientMAccess {
	
	private static final String TABLE_NAME = "lts_admin_job_client_monitor";

	public OracleJobClientMAcess(Config config) {
		super(config);
	}

	@Override
	public void insert(List<JobClientMDataPo> jobClientMDataPos) {
        if (CollectionUtils.isEmpty(jobClientMDataPos)) {
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
                        "submit_success_num",
                        "submit_failed_num",
                        "fail_store_num",
                        "submit_fail_store_num",
                        "handle_feedback_num");

        for (JobClientMDataPo jobClientMDataPo : jobClientMDataPos) {
            insertSql.values(
            		SqlUtils.getIdFromTimestamp(),
                    jobClientMDataPo.getGmtCreated(),
                    jobClientMDataPo.getNodeGroup(),
                    jobClientMDataPo.getIdentity(),
                    jobClientMDataPo.getTimestamp(),
                    jobClientMDataPo.getSubmitSuccessNum(),
                    jobClientMDataPo.getSubmitFailedNum(),
                    jobClientMDataPo.getFailStoreNum(),
                    jobClientMDataPo.getSubmitFailStoreNum(),
                    jobClientMDataPo.getHandleFeedbackNum()
            );
        }
        insertSql.doBatchInsert();
	}

	@Override
	protected String getTableName() {
        return TABLE_NAME;
	}

}
