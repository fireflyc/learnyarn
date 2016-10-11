package im.lsn.learnyarn.am;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;

public class ApplicationMaster {
    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        Configuration conf = new YarnConfiguration();

        //便于使用minicluster调试
        boolean isDebug = false;
        if (args.length >= 1 && args[0].equalsIgnoreCase("debug")) {
            isDebug = true;
        }
        if (isDebug) {

            conf.set(YarnConfiguration.RM_ADDRESS, "localhost:8032");
            conf.set(YarnConfiguration.RM_HOSTNAME, "localhost");
            conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "localhost:8030");
            conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "localhost:8031");
            conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:8088");
            conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        }
        AMRMClientService amrmClientService = new AMRMClientService(conf);
        amrmClientService.start();
        while (!amrmClientService.isStop()) {
            LOG.info(amrmClientService.getCounter());
            Thread.sleep(1000);
        }
        amrmClientService.clean();
    }
}
