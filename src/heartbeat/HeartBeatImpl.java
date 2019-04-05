package heartbeat;

import com.sun.management.UnixOperatingSystemMXBean;
import io.grpc.stub.StreamObserver;
import route.HeartBeatGrpc;
import route.NodeInfo;
import route.Stats;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class HeartBeatImpl extends HeartBeatGrpc.HeartBeatImplBase {
    @Override
    public void isAlive(NodeInfo request, StreamObserver<Stats> responseObserver) {
        sendStatstoMaster(responseObserver);
    }

    public void sendStatstoMaster(StreamObserver<Stats> responseObserver){
        OperatingSystemMXBean mxBean = ManagementFactory.getPlatformMXBean(UnixOperatingSystemMXBean.class);
        Stats.Builder stats = Stats.newBuilder();
        stats.setCpuUsage(Double.toString(((UnixOperatingSystemMXBean) mxBean).getSystemCpuLoad()));
        stats.setDiskSpace(Double.toString(((UnixOperatingSystemMXBean) mxBean).getFreePhysicalMemorySize()));

        responseObserver.onNext(stats.build());
        responseObserver.onCompleted();
    }
}
