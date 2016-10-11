package im.lsn.learnyarn.am;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class AMRMClientService implements AMRMClientAsync.CallbackHandler {
    private static final Log LOG = LogFactory.getLog(AMRMClientService.class);

    private AtomicBoolean stop = new AtomicBoolean(false);
    private ContainerCounter counter = new ContainerCounter();
    private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager;
    private NMClientAsync nodeManager;


    public AMRMClientService(Configuration conf) {
        //得到RM大王的电话号码
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, this);
        resourceManager.init(conf);
        resourceManager.start();
        //得到NM大人的电话号码
        nodeManager = new NMClientAsyncImpl(new NodeManagerContainerHandler(counter));
        nodeManager.init(conf);
        nodeManager.start();
    }

    public void start() throws IOException, YarnException {
        //向RM大王报告AM的状态
        RegisterApplicationMasterResponse registration = resourceManager.registerApplicationMaster("", -1, "");

        Resource clusterMax = registration.getMaximumResourceCapability();
        //当前集群的可用资源
        LOG.info(String.format("max mem=%s, max vcpu=%s", clusterMax.getMemory(), clusterMax.getVirtualCores()));

        //申请资源，这里模拟申请5个资源。RM不会一次分配给我们9个而是在有资源的时候尝试分配给我们一个。
        for (int i = 0; i < 5; i++) {
            //申请资源
            Resource resource = Records.newRecord(Resource.class);
            resource.setMemory(Math.min(clusterMax.getMemory(), 1024));
            resource.setVirtualCores(Math.min(clusterMax.getVirtualCores(), 1));

            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);
            AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(
                    resource,
                    null, // nodes保存node为空是让RM帮我们选
                    null, // racks保存Racks为空是让RM帮我们选择
                    priority);
            resourceManager.addContainerRequest(containerRequest);
            counter.totalRequest.incrementAndGet();//记录请求的个数
        }

    }

    protected void setupLaunchContextTokens(ContainerLaunchContext ctx) {
        try {
            Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            Iterator<org.apache.hadoop.security.token.Token<?>> iterator = credentials.getAllTokens().iterator();
            while (iterator.hasNext()) {
                org.apache.hadoop.security.token.Token<?> token = iterator.next();
                LOG.info("Token " + token);
                if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                    iterator.remove();
                }
            }
            ByteBuffer allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            ctx.setTokens(allTokens);
        } catch (IOException e) {
            LOG.error("Error setting tokens for launch context", e);
        }
    }


    public boolean isStop() {
        //停止条件，主动设置停止标志或者（成功数量+失败数量）==请求分配的总数
        return stop.get() || (counter.totalFailures.get() + counter.totalSuccess.get() == counter.totalRequest.get());
    }

    //当有容器已经完成（无论成功或者失败）都会调用这个方法
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        //如果你要尝试重启某些任务可以在这里做
        //为了演示我这里只累加计数器
        for (ContainerStatus status : statuses) {
            int exitStatus = status.getExitStatus();
            //Container return code 非0表示失败
            if (0 != exitStatus) {
                LOG.warn(String.format("容器挂了 ContainerID=%s Diagnostics=%s", status.getContainerId(),
                        status.getDiagnostics()));
                counter.totalFailures.incrementAndGet();
            } else {
                counter.totalSuccess.incrementAndGet();
            }
        }
    }

    //当容器已经分配好了，都会调用这个方法
    @Override
    public void onContainersAllocated(List<Container> containers) {
        LOG.info("少年,RM大王分配给你了 " + containers.size() + " 个计算单元");
        for (Container allocated : containers) {
            //NM大人，RM大王说让我小的来给你要资源。麻烦您执行一下这个！！

            ContainerLaunchContext launchContext = Records.newRecord(ContainerLaunchContext.class);
            StringBuilder cmd = new StringBuilder();
            cmd.append("ping -c3 www.baidu.com")
                    .append(" ")
                    .append("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                            Path.SEPARATOR + ApplicationConstants.STDOUT)
                    .append(" ")
                    .append("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                            Path.SEPARATOR + ApplicationConstants.STDERR);
            launchContext.setCommands(Collections.singletonList(cmd.toString()));
            setupLaunchContextTokens(launchContext);
            nodeManager.startContainerAsync(allocated, launchContext);//这一这是一个异步，执行完成之后会调用handler的onContainerStarted方法
        }
    }

    //当yarn kill的时候会调用这个刚发
    @Override
    public void onShutdownRequest() {
        stop.set(true);
    }

    //这个方法很美好，意思是指集群中增加了新的NodeManager（扩容）但是现在Hadoop还不支持"在线"伸缩
    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    //这就是在Client显示的进度条咯
    @Override
    public float getProgress() {
        //完成数量/请求分配的总数
        return (counter.totalSuccess.get() + counter.totalFailures.get()) / (float) counter.totalRequest.get();
    }

    //本地的线程挂掉的时候会调用这个，如果你要提高可用性可以尝试在这里重启AMRMClient
    @Override
    public void onError(Throwable e) {
        LOG.error(e.getMessage(), e);
        stop.set(true);
    }

    public ContainerCounter getCounter() {
        return counter;
    }

    //做一些清理工作，主要是向RM大人报告自己到底有没有执行成功
    public void clean() throws IOException, YarnException {
        //停止接收NM大人的电话
        nodeManager.stop();
        if (stop.get() || counter.totalFailures.get() != 0) {
            resourceManager.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "他娘的失败了", null);
        } else {
            resourceManager.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "成功了！！！", null);
        }
        //停止接收RM大王的电话
        resourceManager.stop();
    }
}
