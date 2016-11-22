package io.github.bitornot.lts.ext.queue.oracle;

import io.github.bitornot.lts.ext.queue.oracle.support.RshHolder;
import io.github.bitornot.lts.ext.store.jdbc.builder.DeleteSql;
import io.github.bitornot.lts.ext.store.jdbc.builder.DropTableSql;
import io.github.bitornot.lts.ext.store.jdbc.builder.InsertSql;
import io.github.bitornot.lts.ext.store.jdbc.builder.SelectSql;
import io.github.bitornot.lts.ext.store.jdbc.oracle.OracleJdbcAbstractAccess;
import io.github.bitornot.lts.ext.utils.SqlUtils;

import java.math.BigDecimal;
import java.util.List;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.queue.JobFeedbackQueue;
import com.github.ltsopensource.queue.domain.JobFeedbackPo;
import com.github.ltsopensource.store.jdbc.builder.OrderByType;

/**
 * created by fanlu on 11/14/2016
 */
public class OracleJobFeedbackQueue extends OracleJdbcAbstractAccess implements JobFeedbackQueue {

    public OracleJobFeedbackQueue(Config config) {
        super(config);
    }

    @Override
    public boolean createQueue(String jobClientNodeGroup) {
    	String tableName = getTableName(jobClientNodeGroup);
    	createTable(readSqlFile("sql/oracle/lts_job_feedback_queue.sql", tableName), tableName);
        return true;
    }

    @Override
    public boolean removeQueue(String jobClientNodeGroup) {
        return new DropTableSql(getSqlTemplate())
                .drop(JobQueueUtils.getFeedbackQueueName(jobClientNodeGroup))
                .doDrop();
    }

    private String getTableName(String jobClientNodeGroup) {
        return JobQueueUtils.getFeedbackQueueName(jobClientNodeGroup);
    }

    @Override
    public boolean add(List<JobFeedbackPo> jobFeedbackPos) {
        if (CollectionUtils.isEmpty(jobFeedbackPos)) {
            return true;
        }
        // insert ignore duplicate record
        for (JobFeedbackPo jobFeedbackPo : jobFeedbackPos) {
            String jobClientNodeGroup = jobFeedbackPo.getJobRunResult().getJobMeta().getJob().getSubmitNodeGroup();
            new InsertSql(getSqlTemplate())
                    .insert(getTableName(jobClientNodeGroup))
                    .columns("id", "gmt_created", "job_result")
                    .values(SqlUtils.getIdFromTimestamp(), jobFeedbackPo.getGmtCreated(), JSON.toJSONString(jobFeedbackPo.getJobRunResult()))
                    .doInsert();
        }
        return true;
    }

    @Override
    public boolean remove(String jobClientNodeGroup, String id) {
        return new DeleteSql(getSqlTemplate())
                .delete()
                .from()
                .table(getTableName(jobClientNodeGroup))
                .where("id = ?", id)
                .doDelete() == 1;
    }

    @Override
    public long getCount(String jobClientNodeGroup) {
        return ((BigDecimal) new SelectSql(getSqlTemplate())
                .select()
                .columns("count(1)")
                .from()
                .table(getTableName(jobClientNodeGroup))
                .single()).longValue();
    }

    @Override
    public List<JobFeedbackPo> fetchTop(String jobClientNodeGroup, int top) {

        return new SelectSql(getSqlTemplate())
                .select()
                .all()
                .from()
                .table(getTableName(jobClientNodeGroup))
                .orderBy()
                .column("gmt_created", OrderByType.ASC)
                .limit(0, top)
                .list(RshHolder.JOB_FEED_BACK_LIST_RSH);
    }


}
