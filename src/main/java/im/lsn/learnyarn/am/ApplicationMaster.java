package im.lsn.learnyarn.am;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class ApplicationMaster {
    public static void main(String[] args) throws InterruptedException, IOException, YarnException {
        Configuration conf = new YarnConfiguration();
        AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager = AMRMClientAsync.createAMRMClientAsync(1000,
                new ApplicationMasterCallback());
        resourceManager.init(conf);
        resourceManager.start();

        RegisterApplicationMasterResponse registration = resourceManager.registerApplicationMaster("", -1, "");
        while (true) {
            System.out.println("(stdout)Hello World");
            System.err.println("(stderr)Hello World");
            Thread.sleep(1000);
        }
    }
}
