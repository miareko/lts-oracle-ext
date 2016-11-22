package io.github.bitornot.lts.ext.monitor.access.oracle;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.monitor.access.MonitorAccessFactory;
import com.github.ltsopensource.monitor.access.face.JVMGCAccess;
import com.github.ltsopensource.monitor.access.face.JVMMemoryAccess;
import com.github.ltsopensource.monitor.access.face.JVMThreadAccess;
import com.github.ltsopensource.monitor.access.face.JobClientMAccess;
import com.github.ltsopensource.monitor.access.face.JobTrackerMAccess;
import com.github.ltsopensource.monitor.access.face.TaskTrackerMAccess;

/**
 * created by fanlu on 11/15/2016
 */
public class OracleMonitorAccessFactory implements MonitorAccessFactory {

	@Override
	public JobTrackerMAccess getJobTrackerMAccess(Config config) {
		return new OracleJobTrackerMAccess(config);
	}

	@Override
	public TaskTrackerMAccess getTaskTrackerMAccess(Config config) {
		return new OracleTaskTrackerMAccess(config);
	}

	@Override
	public JVMGCAccess getJVMGCAccess(Config config) {
		return new OracleJVMGCAccess(config);
	}

	@Override
	public JVMMemoryAccess getJVMMemoryAccess(Config config) {
		return new OracleJVMMemoryAccess(config);
	}

	@Override
	public JVMThreadAccess getJVMThreadAccess(Config config) {
		return new OracleJVMThreadAccess(config);
	}

	@Override
	public JobClientMAccess getJobClientMAccess(Config config) {
		return new OracleJobClientMAcess(config);
	}

}
