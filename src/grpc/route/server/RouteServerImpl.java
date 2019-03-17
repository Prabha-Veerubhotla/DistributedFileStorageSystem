package grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.lang.*;
import com.google.protobuf.ByteString;
import lease.Dhcp_Lease_Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.FetchConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc.RouteServiceImplBase;


public class RouteServerImpl extends RouteServiceImplBase {
	protected static Logger logger = LoggerFactory.getLogger("server");
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
		logger.info("-- got: " + msg.getOrigin() + ", path: " + msg.getPath() + ", with: " + content);

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
				logger.info("Starting DHCP Lease Monitor Thread...");
					new Dhcp_Lease_Test().monitorLease();
			}
		};
		thread.start();
	}

	private void start() throws Exception {
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteServerImpl())
				.build();

		logger.info("-- starting server");
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

	private route.Route buildError(route.Route request, String msg) {
		route.Route.Builder builder = route.Route.newBuilder();

		// routing/header information
		builder.setId(RouteServer.getInstance().getNextMessageID());
		builder.setOrigin(RouteServer.getInstance().getServerID());
		builder.setSeqnum(-1);
		builder.setDestination(request.getOrigin());
		builder.setPath(request.getPath());

		// do the work
		builder.setPayload(process(request));

		route.Route rtn = builder.build();

		return rtn;
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

	@Override
	public void requestStreamFrom(route.Route request, StreamObserver<route.Route> responseObserver) {
		route.Route.Builder builder = route.Route.newBuilder();

		// TODO accept input from request as to what to retrieve. For now it is -- done
		// hard-coded to a fixed file

		String filename = new String(request.getPayload().toByteArray());
		File fn = new File(filename);

		if (!fn.exists()) {
			route.Route rtn = buildError(request, "unknown file");
			responseObserver.onNext(rtn);
			responseObserver.onCompleted();
		}

		FileInputStream fis = null;
		try {
			fis = new FileInputStream(fn);
			long seq = 0l;
			final int blen = 1024;
			byte[] raw = new byte[blen];
			boolean done = false;
			while (!done) {
				int n = fis.read(raw, 0, blen);
				if (n <= 0)
					break;

				// identifying sequence number
				seq++;

				// routing/header information
				builder.setId(RouteServer.getInstance().getNextMessageID());
				builder.setOrigin(RouteServer.getInstance().getServerID());
				builder.setDestination(request.getOrigin());
				builder.setSeqnum(seq);
				builder.setPath(request.getPath());
				builder.setPayload(ByteString.copyFrom(raw, 0, n));

				route.Route rtn = builder.build();
				responseObserver.onNext(rtn);
			}
		} catch (IOException e) {
			; // ignore? really?
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				; // ignore
			}

			responseObserver.onCompleted();
		}

	}
	//TODO: add sending a message without getting a request
	//make it more interactive
}
