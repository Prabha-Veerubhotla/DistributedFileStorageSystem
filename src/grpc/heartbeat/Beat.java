package grpc.heartbeat;

import java.lang.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.io.File;
import java.io.IOException;
import utility.FetchConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.concurrent.CountDownLatch;
import com.sun.management.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import route.Route;
import route.RouteServiceGrpc;

import lease.Dhcp_Lease_Test;
import heartbeat.Heartbeat;
import heartbeat.HeartBeatServiceGrpc;
import heartbeat.NodeInfo;
import heartbeat.stats;
// import consensus.Consensus;
// import consensus.ConsensusServiceGrpc;
import heartbeat.HeartBeatServiceGrpc.HeartBeatServiceImplBase;


public class Beat extends HeartBeatServiceImplBase{
    protected static Logger logger = LoggerFactory.getLogger("server");
    private static HeartBeatServiceGrpc.HeartBeatServiceStub stub;
    private static ManagedChannel ch;
    // Consensus response;
    Heartbeat heartbeat;

    OperatingSystemMXBean mxBean = ManagementFactory.getPlatformMXBean(UnixOperatingSystemMXBean.class);

    public void start(String myIp){

        // NodeInfo.Builder ni = NodeInfo.newBuilder();
        // ni.setIp(myIp);
        // ni.setPort("2345");

        ch = ManagedChannelBuilder.forAddress("127.0.0.1", Integer.parseInt("2345")).usePlaintext(true).build();
        //TODO: make it async stub
        stub = HeartBeatServiceGrpc.newStub(ch);
        
        stats.Builder sb = stats.newBuilder();
        CountDownLatch latch = new CountDownLatch(1);
        System.out.println("test");
        StreamObserver<stats> observer = stub.requestStats(new StreamObserver<stats>() {
            @Override
            public void onNext(stats st){
                logger.info(st.getCpuUsage());
                logger.info(st.getMemUsage());
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("Exception in the response from server: " + throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Server is done sending data");
                latch.countDown();
            }
        });

        TimerTask beat = new Beat_Worker(){
            protected void beat(){
                sb.setCpuUsage(Double.toString(mxBean.getSystemCpuLoad()*100));
                sb.setMemUsage(Double.toString(mxBean.getFreePhysicalMemorySize()));
                observer.onNext(sb.build());
            }
        };
        Timer timer = new  Timer();
        timer.schedule(beat, new Date(), 5000);
    }

}