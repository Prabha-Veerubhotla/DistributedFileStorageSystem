package grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.lang.*;
import com.google.protobuf.ByteString;
import lease.Dhcp_Lease_Test;
import utility.FetchConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc.RouteServiceImplBase;


public class RouteServerImpl extends RouteServiceImplBase {
	private Server svr;
	/**
	 * TODO refactor this!
	 * 
	 * @param path
	 * @param payload
	 * @return
	 */
	protected ByteString process(route.Route msg) {

		// TODO placeholder
		String content = new String(msg.getPayload().toByteArray());
		System.out.println("-- got: " + msg.getOrigin() + ", path: " + msg.getPath() + ", with: " + content);

		// TODO complete processing
		final String blank = "blank";
		byte[] raw = blank.getBytes();

		return ByteString.copyFrom(raw);
	}

	public static void main(String[] args) throws Exception {
        if(args.length == 0) {
        	System.out.println("Missing server configuration");
        	return;
		}
		String path = args[0];
		Properties conf = FetchConfig.getConfiguration(new File(path));
		RouteServer.configure(conf);

		final RouteServerImpl impl = new RouteServerImpl();
		impl.start();
		impl.blockUntilShutdown();
	}

	private void invokeDhcpMonitorThread() {
		Thread thread = new Thread(){
			public void run(){
				System.out.println("Starting DHCP Lease Monitor Thread...");
					new Dhcp_Lease_Test().monitorLease();
			}
		};
		thread.start();
	}

	private void start() throws Exception {
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteServerImpl())
				.build();

		System.out.println("-- starting server");
		svr.start();
		invokeDhcpMonitorThread();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				RouteServerImpl.this.stop();
			}
		});
	}

	protected void stop() {
		svr.shutdown();
	}

	private void blockUntilShutdown() throws Exception {
		svr.awaitTermination();
	}

	//respond to a request
	@Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {

		// TODO refactor to use RouteServer to isolate implementation from
		// transportation

		route.Route.Builder builder = route.Route.newBuilder();

		// routing/header information
		builder.setId(RouteServer.getInstance().getNextMessageID());
		builder.setOrigin(RouteServer.getInstance().getServerID());
		builder.setDestination(request.getOrigin());
		builder.setPath(request.getPath());

		// do the work
		builder.setPayload(process(request));

		route.Route rtn = builder.build();
		responseObserver.onNext(rtn);
		responseObserver.onCompleted();
	}

	//TODO: add sending a message without getting a request
	//make it more interactive
}
