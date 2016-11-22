package io.github.bitornot.lts.ext.monitor.access.oracle;

import io.github.bitornot.lts.ext.store.jdbc.builder.InsertSql;
import io.github.bitornot.lts.ext.utils.SqlUtils;

import java.util.List;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.monitor.access.domain.JVMMemoryDataPo;
import com.github.ltsopensource.monitor.access.face.JVMMemoryAccess;

/**
 * created by fanlu on 11/15/2016
 */
public class OracleJVMMemoryAccess extends OracleAbstractAccess implements JVMMemoryAccess {

	private static final String TABLE_NAME = "lts_admin_jvm_memory";
	
	public OracleJVMMemoryAccess(Config config) {
		super(config);
	}

	@Override
	public void insert(List<JVMMemoryDataPo> jvmMemoryDataPos) {

        if (CollectionUtils.isEmpty(jvmMemoryDataPos)) {
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
                        "heap_memory_committed",
                        "heap_memory_init",
                        "heap_memory_max",
                        "heap_memory_used",
                        "non_heap_memory_committed",
                        "non_heap_memory_init",
                        "non_heap_memory_max",
                        "non_heap_memory_used",
                        "perm_gen_committed",
                        "perm_gen_init",
                        "perm_gen_max",
                        "perm_gen_used",
                        "old_gen_committed",
                        "old_gen_init",
                        "old_gen_max",
                        "old_gen_used",
                        "eden_space_committed",
                        "eden_space_init",
                        "eden_space_max",
                        "eden_space_used",
                        "survivor_committed",
                        "survivor_init",
                        "survivor_max",
                        "survivor_used");
        for (JVMMemoryDataPo po : jvmMemoryDataPos) {
            insertSql.values(
            		SqlUtils.getIdFromTimestamp(),
                    po.getGmtCreated(),
                    po.getIdentity(),
                    po.getTimestamp(),
                    po.getNodeType().name(),
                    po.getNodeGroup(),
                    po.getHeapMemoryCommitted(),
                    po.getHeapMemoryInit(),
                    po.getHeapMemoryMax(),
                    po.getHeapMemoryUsed(),
                    po.getNonHeapMemoryCommitted(),
                    po.getNonHeapMemoryInit(),
                    po.getNonHeapMemoryMax(),
                    po.getNonHeapMemoryUsed(),
                    po.getPermGenCommitted(),
                    po.getPermGenInit(),
                    po.getPermGenMax(),
                    po.getPermGenUsed(),
                    po.getOldGenCommitted(),
                    po.getOldGenInit(),
                    po.getOldGenMax(),
                    po.getOldGenUsed(),
                    po.getEdenSpaceCommitted(),
                    po.getEdenSpaceInit(),
                    po.getEdenSpaceMax(),
                    po.getEdenSpaceUsed(),
                    po.getSurvivorCommitted(),
                    po.getSurvivorInit(),
                    po.getSurvivorMax(),
                    po.getSurvivorUsed()
            );
        }

        insertSql.doBatchInsert();
	}

	@Override
	protected String getTableName() {
        return TABLE_NAME;
	}

}
