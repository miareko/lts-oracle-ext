package io.github.bitornot.lts.ext.biz.log.oracle;

import com.github.ltsopensource.biz.logger.JobLogger;
import com.github.ltsopensource.biz.logger.JobLoggerFactory;
import com.github.ltsopensource.core.cluster.Config;

/**
 * created by fanlu on 11/14/2016
 */
public class OracleJobLoggerFactory implements JobLoggerFactory {

	@Override
	public JobLogger getJobLogger(Config config) {
		return new OracleJobLogger(config);
	}

}
