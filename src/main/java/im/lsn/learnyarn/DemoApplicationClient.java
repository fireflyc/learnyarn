package im.lsn.learnyarn;

import im.lsn.learnyarn.am.ApplicationMaster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.nio.ByteBuffer;
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
        FileSystem fs = FileSystem.get(conf);

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        Resource clusterMax = appResponse.getMaximumResourceCapability(); //集群最大资源

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

        //运行AM
        ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);

        //定义资源
        Resource amResource = Records.newRecord(Resource.class);
        amResource.setMemory(Math.min(clusterMax.getMemory(), 1));
        amResource.setVirtualCores(Math.min(clusterMax.getVirtualCores(), 4));
        appContext.setResource(amResource);

        //定义执行的代码
        StringBuilder cmd = new StringBuilder();
        cmd.append("\"" + ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java\"")
                .append(" ")
                .append(appMasterMainClass)
                .append(" ");
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            cmd.append(" ").append("debug").append(" ");
        }
        cmd.append("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDOUT)
                .append(" ")
                .append("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDERR);
        if(LOG.isInfoEnabled()) {
            LOG.info("AM " + cmd.toString());
        }
        clc.setCommands(Collections.singletonList(cmd.toString()));
        //添加执行的Jar
        Map<String, LocalResource> localResourceMap = new HashMap<String, LocalResource>();
        File appMasterJarFile = new File(appMasterJar);
        localResourceMap.put(appMasterJarFile.getName(), toLocalResource(fs, appResponse.getApplicationId().toString(),
                appMasterJarFile));
        clc.setLocalResources(localResourceMap);
        appContext.setAMContainerSpec(clc);


        Map<String, String> envMap = new HashMap<String, String>();
        envMap.put("CLASSPATH", hadoopClassPath());
        envMap.put("LANG", "en_US.UTF-8");
        clc.setEnvironment(envMap);

        //token
        if (UserGroupInformation.isSecurityEnabled()) {
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
            }
            Credentials credentials = new Credentials();
            org.apache.hadoop.security.token.Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
            if (LOG.isInfoEnabled()) {
                if (tokens != null) {
                    for (org.apache.hadoop.security.token.Token<?> token : tokens) {
                        LOG.info("Got dt for " + fs.getUri() + "; " + token);
                    }
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            clc.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
        }

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
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        return classPathEnv.toString();
    }


    protected Path copyToHdfs(FileSystem fs, String appId, String srcFilePath) throws IOException {
        Path src = new Path(srcFilePath);
        String suffix = ".staging" + File.separator + appId + File.separator + src.getName();
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (!fs.exists(dst.getParent())) {
            FileSystem.mkdirs(fs, dst.getParent(), FsPermission.createImmutable((short) Integer.parseInt("755", 8)));
        }
        fs.copyFromLocalFile(src, dst);
        return dst;
    }

    private LocalResource toLocalResource(FileSystem fs, String appId, File file) throws IOException {
        Path hdfsFile = copyToHdfs(fs, appId, file.getPath());
        FileStatus stat = fs.getFileStatus(hdfsFile);
        return LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(hdfsFile.toUri()),
                LocalResourceType.FILE, LocalResourceVisibility.PRIVATE, stat.getLen(), stat.getModificationTime());
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
