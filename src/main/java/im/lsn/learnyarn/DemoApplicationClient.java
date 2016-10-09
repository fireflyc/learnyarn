package im.lsn.learnyarn;

import im.lsn.learnyarn.am.ApplicationMaster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DemoApplicationClient {
    private static Log LOG = LogFactory.getLog(DemoApplicationClient.class);
    private final Configuration conf;
    private final String appMasterMainClass;
    private final YarnClient yarnClient;

    private String appMasterJar;

    public DemoApplicationClient(Configuration conf) {
        this.conf = conf;
        this.appMasterJar = ClassUtil.findContainingJar(ApplicationMaster.class);
        this.appMasterMainClass = ApplicationMaster.class.getName();

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
    }

    public void setAppMasterJar(String appMasterJar) {
        this.appMasterJar = appMasterJar;
    }

    public ApplicationId submit() throws IOException, YarnException {
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        Resource clusterMax = appResponse.getMaximumResourceCapability(); //集群最大资源

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

        //运行AM
        ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);

        //定义资源
        Resource amResource = Records.newRecord(Resource.class);
        amResource.setMemorySize(Math.min(clusterMax.getMemorySize(), 1));
        amResource.setVirtualCores(Math.min(clusterMax.getVirtualCores(), 4));
        appContext.setResource(amResource);

        //定义执行的代码
        StringBuilder cmd = new StringBuilder();
        cmd.append("\"" + ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java\"")
                .append(" ")
                .append(appMasterMainClass)
                .append(" ")
                .append("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        Path.SEPARATOR + ApplicationConstants.STDOUT)
                .append(" ")
                .append("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        Path.SEPARATOR + ApplicationConstants.STDERR);
        clc.setCommands(Collections.singletonList(cmd.toString()));
        //添加执行的Jar
        Map<String, LocalResource> localResourceMap = new HashMap<String, LocalResource>();
        File appMasterJarFile = new File(appMasterJar);
        localResourceMap.put(appMasterJarFile.getName(), toLocalResource(appMasterJarFile));
        clc.setLocalResources(localResourceMap);
        appContext.setAMContainerSpec(clc);

        Map<String, String> envMap = new HashMap<String, String>();
        envMap.put("CLASSPATH", hadoopClassPath());
        envMap.put("LANG", "en_US.UTF-8");
        clc.setEnvironment(envMap);
        //提交
        return yarnClient.submitApplication(appContext);
    }

    public ApplicationReport getApplicationReport(ApplicationId appId) throws IOException, YarnException {
        return yarnClient.getApplicationReport(appId);
    }

    protected String hadoopClassPath() {
        StringBuilder classPathEnv = new StringBuilder().append(File.pathSeparatorChar).append("./*");
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        return classPathEnv.toString();
    }

    private LocalResource toLocalResource(File file) {
        return LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(file.toURI()),
                LocalResourceType.FILE, LocalResourceVisibility.PRIVATE, file.length(), file.lastModified());
    }

    public static void main(String args[]) throws IOException, YarnException, InterruptedException {
        Configuration conf = new YarnConfiguration();
        DemoApplicationClient client = new DemoApplicationClient(conf);
        ApplicationId applicationId = client.submit();
        boolean outAccepted = false;
        boolean outTrackingUrl = false;
        ApplicationReport report = client.getApplicationReport(applicationId);
        while (report.getYarnApplicationState() != YarnApplicationState.FINISHED) {
            report = client.getApplicationReport(applicationId);
            if (!outAccepted && report.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
                LOG.info("Application is accepted use Queue=" + report.getQueue()
                        + " applicationId=" + report.getApplicationId());
                outAccepted = true;
            }
            if (!outTrackingUrl && report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
                String trackingUrl = report.getTrackingUrl();
                LOG.info("Master Tracking URL = " + trackingUrl);
                outTrackingUrl = true;
            }
            LOG.info(String.format("%f %s", report.getProgress(), report.getYarnApplicationState()));
            Thread.sleep(1000);
        }
        LOG.info(report.getFinalApplicationStatus());
    }
}
