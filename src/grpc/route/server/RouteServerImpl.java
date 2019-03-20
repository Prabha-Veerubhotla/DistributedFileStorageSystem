package grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
	private String name;
	private static boolean isMaster = false;
	private static String origin = "slave";
	private static String destination = "master";
	private static String myIp;
	private static List<String> slaveips = new ArrayList<>();
	private static Dhcp_Lease_Test dhcp_lease_test = new Dhcp_Lease_Test();
	/**
	 * TODO refactor this!
	 * 
	 * @param path
	 * @param payload
	 * @return
	 */
	protected ByteString processMaster(route.Route msg) {
        String reply = null;
        if(msg.getType().equalsIgnoreCase( "join")) {
        	name = new String(msg.getPayload().toByteArray());
			logger.info("--> join: " + name);
			reply = "Hello "+new String(msg.getPayload().toByteArray())+ "!";

		} else if(msg.getType().equalsIgnoreCase( "message-put")) {
        	String message = new String(msg.getPayload().toByteArray());
			logger.info("--> message from: " + name +" asking to save: "+message);

			if(MasterNode.saveMessage(message, name)) {
				reply = "success";
			} else {
               reply = "failure";
			}

		} else if(msg.getType().equalsIgnoreCase( "message-get")) {
			String message = new String(msg.getPayload().toByteArray());
			logger.info("--> message from: " + name +" asking to retrieve: "+message);
			reply = MasterNode.getMessage(message, name);
		}
		else if(msg.getType().equalsIgnoreCase( "message-delete")) {
			String message = new String(msg.getPayload().toByteArray());
			logger.info("--> message from: " + name +" asking to delete: "+message);
			if(MasterNode.deleteMessage(message, name)) {
				reply = "success";
			} else {
				reply = "failure";
			}
		}
		else if(msg.getType().equalsIgnoreCase( "message-list")) {
			//String message = new String(msg.getPayload().toByteArray());
			logger.info("--> message from: " + name +" asking to list all messages");
			reply = MasterNode.listMessages(name);
		}

		else if(msg.getType().equalsIgnoreCase( "request-ip")) {
			//String message = new String(msg.getPayload().toByteArray());
			logger.info("--> message from: " + name +" asking to assign ip");
			reply = MasterNode.sendIpToClient(dhcp_lease_test.getCurrentNodeMapping(), dhcp_lease_test.getCurrentIpList());
		}

		else if(msg.getType().equalsIgnoreCase( "save-client-ip")) {
			String message = new String(msg.getPayload().toByteArray());
			logger.info("--> message from: " + name +" asking to save  client ip");
			if(dhcp_lease_test.updateCurrentNodeMapping(message, msg.getOrigin())) {
				reply = "success";
			} else {
				reply = "failure";
			}

		}

		else {

			// TODO placeholder
			String content = new String(msg.getPayload().toByteArray());
			logger.info("-- got: " + msg.getOrigin() + ", path: " + msg.getPath() + ", with: " + content);
			reply = "blank";
		}

		byte[] raw = reply.getBytes();
		return ByteString.copyFrom(raw);
	}


	public void getSlaveIpList() {
		Map<String, List<String>> map = dhcp_lease_test.getCurrentNodeMapping();
		if(map.containsKey("slave")) {
			slaveips = map.get("slave");
		}
		MasterNode.assignSlaveIp(slaveips);
	}

	protected ByteString processSlave(route.Route msg) {
		String reply = "null";
		if(msg.getType().equalsIgnoreCase("message-save")) {
			String actualmessage = new String(msg.getPayload().toByteArray());
			String name = msg.getUsername();
			if(SlaveNode.saveMessage(actualmessage, name)) {
				logger.info("--saved message: " + actualmessage + " from: " + name + " successfully");
				reply = "success";
			} else {
				reply = "failure";
				logger.info( "--unable to save message: " + actualmessage + "from: " + name);
			}
		}
		if(msg.getType().equalsIgnoreCase("message-get")) {
			String actualmessage = new String(msg.getPayload().toByteArray());
			String name = msg.getUsername();
			reply = SlaveNode.getSavedMessage(actualmessage, name);
			logger.info("retrieving information of "+name);

		}
		if(msg.getType().equalsIgnoreCase("message-delete")) {
			String actualmessage = new String(msg.getPayload().toByteArray());
			String name = msg.getUsername();
			if(SlaveNode.deleteMessage(actualmessage, name)) {
				reply = "success";
			} else {
				reply = "failure";
			}


		}
		if(msg.getType().equalsIgnoreCase("message-list")) {
			String actualmessage = new String(msg.getPayload().toByteArray());
			String name = msg.getUsername();
			reply = SlaveNode.listMessages(name).toString();
		}
		if(msg.getType().equalsIgnoreCase("node-ip")) {
			String actualmessage = new String(msg.getPayload().toByteArray());
			String name = msg.getUsername();
			myIp = actualmessage;
			reply = "slave";
		}

		if(reply == null) {
			reply = "";
		}
		byte[] raw = reply.getBytes();
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
		if(conf.getProperty("server.name").equalsIgnoreCase("master")) {
			isMaster = true;
			logger.info("Running as master node on: 127.0.0.1 :"+  conf.getProperty("server.port"));
		} else {
			logger.info("Running as slave node on: 127.0.0.1 :"+  conf.getProperty("server.port"));
		}
		impl.start();
		impl.blockUntilShutdown();
	}

	private void invokeDhcpMonitorThread() {
		Thread thread = new Thread(){
			public void run(){
				logger.info("Starting DHCP Lease Monitor Thread...");
					dhcp_lease_test.monitorLease();
			}
		};
		thread.start();
	}

	private void start() throws Exception {
		svr = ServerBuilder.forPort(RouteServer.getInstance().getServerPort()).addService(new RouteServerImpl())
				.build();

		logger.info("-- starting server");
		svr.start();
		if(isMaster) {
			invokeDhcpMonitorThread();
		}


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
		//builder.setId(RouteServer.getInstance().getNextMessageID());
		//builder.setOrigin(RouteServer.getInstance().getServerID());
		//builder.setSeqnum(-1);

		//builder.setDestination(request.getOrigin());
		builder.setPath(request.getPath());

		// do the work
		if(isMaster) {
			builder.setPayload(processMaster(request));
			builder.setOrigin("master");
			builder.setDestination("slave");
		} else {
			builder.setPayload(processSlave(request));
			builder.setOrigin("slave");
			builder.setDestination("master");
		}

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
		//builder.setId(RouteServer.getInstance().getNextMessageID());
		//builder.setOrigin(RouteServer.getInstance().getServerID());
		//builder.setDestination(request.getOrigin());
		builder.setPath(request.getPath());

		// do the work
		if(isMaster) {
			builder.setPayload(processMaster(request));
			builder.setOrigin("master");
			builder.setDestination("slave");
		} else {
            builder.setPayload(processSlave(request));
			builder.setOrigin("slave");
			builder.setDestination("master");
		}

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
		logger.info("-- received file: " +filename+" from: "+name);

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
				//builder.setId(RouteServer.getInstance().getNextMessageID());
				//builder.setOrigin(RouteServer.getInstance().getServerID());
				builder.setOrigin("master");
				builder.setDestination(request.getOrigin());
				//builder.setSeqnum(seq);
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
