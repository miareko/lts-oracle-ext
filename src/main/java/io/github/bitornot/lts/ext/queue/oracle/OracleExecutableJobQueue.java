package io.github.bitornot.lts.ext.queue.oracle;

import io.github.bitornot.lts.ext.queue.oracle.support.RshHolder;
import io.github.bitornot.lts.ext.store.jdbc.builder.DeleteSql;
import io.github.bitornot.lts.ext.store.jdbc.builder.DropTableSql;
import io.github.bitornot.lts.ext.store.jdbc.builder.SelectSql;
import io.github.bitornot.lts.ext.store.jdbc.builder.UpdateSql;

import java.math.BigDecimal;
import java.util.List;

import com.github.ltsopensource.admin.request.JobQueueReq;
import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.support.JobQueueUtils;
import com.github.ltsopensource.core.support.SystemClock;
import com.github.ltsopensource.queue.ExecutableJobQueue;
import com.github.ltsopensource.queue.domain.JobPo;
import com.github.ltsopensource.store.jdbc.exception.TableNotExistException;

/**
 * created by fanlu on 11/14/2016
 */
public class OracleExecutableJobQueue extends AbstractOracleJobQueue implements ExecutableJobQueue {

    public OracleExecutableJobQueue(Config config) {
        super(config);
    }

    @Override
    protected String getTableName(JobQueueReq request) {
        if (StringUtils.isEmpty(request.getTaskTrackerNodeGroup())) {
            throw new IllegalArgumentException(" takeTrackerNodeGroup cat not be null");
        }
        return getTableName(request.getTaskTrackerNodeGroup());
    }

    @Override
    public boolean createQueue(String taskTrackerNodeGroup) {
    	String tableName = getTableName(taskTrackerNodeGroup);
		createTable(readSqlFile("sql/oracle/lts_executable_job_queue.sql", tableName), tableName);
        return true;
    }

    @Override
    public boolean removeQueue(String taskTrackerNodeGroup) {
        return new DropTableSql(getSqlTemplate())
                .drop(JobQueueUtils.getExecutableQueueName(taskTrackerNodeGroup))
                .doDrop();
    }

    private String getTableName(String taskTrackerNodeGroup) {
        return JobQueueUtils.getExecutableQueueName(taskTrackerNodeGroup);
    }

    @Override
    public boolean add(JobPo jobPo) {
        try {
            return super.add(getTableName(jobPo.getTaskTrackerNodeGroup()), jobPo);
        } catch (TableNotExistException e) {
            // 表不存在
            createQueue(jobPo.getTaskTrackerNodeGroup());
            add(jobPo);
        }
        return true;
    }

    @Override
    public boolean remove(String taskTrackerNodeGroup, String jobId) {
        return new DeleteSql(getSqlTemplate())
                .delete()
                .from()
                .table(getTableName(taskTrackerNodeGroup))
                .where("job_id = ?", jobId)
                .doDelete() == 1;
    }

    @Override
    public long countJob(String realTaskId, String taskTrackerNodeGroup) {
        return ((BigDecimal) new SelectSql(getSqlTemplate())
                .select()
                .columns("COUNT(1)")
                .from()
                .table(getTableName(taskTrackerNodeGroup))
                .where("real_task_id = ?", realTaskId)
                .and("task_tracker_node_group = ?", taskTrackerNodeGroup)
                .single()).longValue();
    }

    @Override
    public boolean removeBatch(String realTaskId, String taskTrackerNodeGroup) {
        new DeleteSql(getSqlTemplate())
                .delete()
                .from()
                .table(getTableName(taskTrackerNodeGroup))
                .where("real_task_id = ?", realTaskId)
                .and("task_tracker_node_group = ?", taskTrackerNodeGroup)
                .doDelete();
        return true;
    }

    @Override
    public void resume(JobPo jobPo) {
        new UpdateSql(getSqlTemplate())
                .update()
                .table(getTableName(jobPo.getTaskTrackerNodeGroup()))
                .set("is_running", false)
                .set("task_tracker_identity", null)
                .set("gmt_modified", SystemClock.now())
                .where("job_id=?", jobPo.getJobId())
                .doUpdate();
    }

    @Override
    public List<JobPo> getDeadJob(String taskTrackerNodeGroup, long deadline) {

        return new SelectSql(getSqlTemplate())
                .select()
                .all()
                .from()
                .table(getTableName(taskTrackerNodeGroup))
                .where("is_running = ?", true)
                .and("gmt_modified < ?", deadline)
                .list(RshHolder.JOB_PO_LIST_RSH);
    }

    @Override
    public JobPo getJob(String taskTrackerNodeGroup, String taskId) {
        return new SelectSql(getSqlTemplate())
                .select()
                .all()
                .from()
                .table(getTableName(taskTrackerNodeGroup))
                .where("task_id = ?", taskId)
                .and("task_tracker_node_group = ?", taskTrackerNodeGroup)
                .single(RshHolder.JOB_PO_RSH);
    }
}
