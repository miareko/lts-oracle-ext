package io.github.bitornot.lts.ext.monitor.access.oracle;

import io.github.bitornot.lts.ext.store.jdbc.builder.InsertSql;
import io.github.bitornot.lts.ext.utils.SqlUtils;

import java.util.List;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.monitor.access.domain.JVMGCDataPo;
import com.github.ltsopensource.monitor.access.face.JVMGCAccess;

/**
 * created by fanlu on 11/15/2016
 */
public class OracleJVMGCAccess extends OracleAbstractAccess implements JVMGCAccess {

	private static final String TABLE_NAME = "lts_admin_jvm_gc";

	public OracleJVMGCAccess(Config config) {
		super(config);
	}

	@Override
	public void insert(List<JVMGCDataPo> jvmGCDataPos) {
        if (CollectionUtils.isEmpty(jvmGCDataPos)) {
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
                        "young_gc_collection_count",
                        "young_gc_collection_time",
                        "full_gc_collection_count",
                        "full_gc_collection_time",
                        "span_young_gc_collection_count",
                        "span_young_gc_collection_time",
                        "span_full_gc_collection_count",
                        "span_full_gc_collection_time");

        for (JVMGCDataPo po : jvmGCDataPos) {
            insertSql.values(
            		SqlUtils.getIdFromTimestamp(),
                    po.getGmtCreated(),
                    po.getIdentity(),
                    po.getTimestamp(),
                    po.getNodeType().name(),
                    po.getNodeGroup(),
                    po.getYoungGCCollectionCount(),
                    po.getYoungGCCollectionTime(),
                    po.getFullGCCollectionCount(),
                    po.getFullGCCollectionTime(),
                    po.getSpanYoungGCCollectionCount(),
                    po.getSpanYoungGCCollectionTime(),
                    po.getSpanFullGCCollectionCount(),
                    po.getSpanFullGCCollectionTime()
            );
        }

        insertSql.doBatchInsert();
	}

	@Override
	protected String getTableName() {
        return TABLE_NAME;
	}

}
