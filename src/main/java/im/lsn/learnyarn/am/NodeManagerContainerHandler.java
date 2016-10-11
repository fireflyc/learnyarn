package im.lsn.learnyarn.am;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 老子除了做计数什么也不干没错，唯一有用的方法就是onContainerStarted
 */
public class NodeManagerContainerHandler implements NMClientAsync.CallbackHandler {
    private static final Log LOG = LogFactory.getLog(NodeManagerContainerHandler.class);
    private ContainerCounter counter;

    public NodeManagerContainerHandler(ContainerCounter counter) {
        this.counter = counter;
    }

    //调用startContainerAsync之后返回的消息
    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        counter.totalStarted.incrementAndGet();
    }

    //下面两个方法只要我们不向NM大人发送请求就不会被调用，所以可以忽略。

    //调用getContainerStatusAsync之后返回的消息
    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        if (LOG.isInfoEnabled()) {
            LOG.info("少年，NM大人回消息给你了你的计算单元 containerId=" + containerId + " containerStatus=" + containerStatus);
        }
    }

    //调用stopContainerAsync之后返回的消息
    @Override
    public void onContainerStopped(ContainerId containerId) {
        if (LOG.isInfoEnabled()) {
            LOG.info("你成功干掉一个计算资源 containerId=" + containerId);
        }
    }

    //下面三个错误都是ApplicationMaster抛出，表示发送某个RPC失败
    @Override
    public void onStartContainerError(ContainerId containerId, Throwable throwable) {
        if (LOG.isErrorEnabled()) {
            LOG.error("Start都发不了你是不是和NM大人失去联系了？ containerId=" + containerId, throwable);
        }
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
        if (LOG.isErrorEnabled()) {
            LOG.error("发个RPC都出错，Hadoop不给力啊 containerId=" + containerId, throwable);
        }
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable throwable) {
        if (LOG.isErrorEnabled()) {
            LOG.error("关个进程都挂掉，Hadoop不给力啊 containerId=" + containerId, throwable);
        }
    }
}
