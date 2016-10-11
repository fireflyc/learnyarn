package im.lsn.learnyarn.am;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 计数器，纯粹为了统计Container的数量
 */
public class ContainerCounter {
    public AtomicInteger totalRequest = new AtomicInteger();//申请资源总数
    public AtomicInteger totalStarted = new AtomicInteger();//已经启动的次数(无论成功或者失败)
    public AtomicInteger totalFailures = new AtomicInteger();//失败数量
    public AtomicInteger totalSuccess = new AtomicInteger();//成功数量
    //当前已经完成数量=成功数量+失败数量
    //当前正在执行的数量=已经启动的次数-成功数量-失败数量

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ContainerCounter{");
        sb.append("totalRequest=").append(totalRequest);
        sb.append(", totalStarted=").append(totalStarted);
        sb.append(", totalFailures=").append(totalFailures);
        sb.append(", totalSuccess=").append(totalSuccess);
        sb.append('}');
        return sb.toString();
    }
}
