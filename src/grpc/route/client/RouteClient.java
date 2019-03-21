package grpc.route.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * copyright 2018, gash
 *
 * Gash licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

//TODO: make all calls aynschronous

public class RouteClient {
	private static long clientID = 501;
	private static ManagedChannel ch;
	private static RouteServiceGrpc.RouteServiceBlockingStub stub;
	private Properties setup;
	private static String name;
	private Route.Builder bld;
	private Route r;
	private static String origin = "client";
	private static String destination = "master";
	private static String myIp;


	public RouteClient(Properties setup) {
		this.setup = setup;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void startClientSession() {
		String host = setup.getProperty("host");
		String port = setup.getProperty("port");
		if (host == null || port == null) {
			throw new RuntimeException("Missing port and/or host");
		}
		ch = ManagedChannelBuilder.forAddress(host, Integer.parseInt(port)).usePlaintext(true).build();
		//async stub
		stub = RouteServiceGrpc.newBlockingStub(ch);
		System.out.println("Client running...");

		bld = Route.newBuilder();
	}

	 //receive message from server and respond
     public void respond() {
		 // TODO response handling
		 String payload = new String(r.getPayload().toByteArray());
		 System.out.println("reply: " + payload + ", from: " + r.getOrigin() );
	 }

	 public void join(String name) {
		//TODO: send message to server that client with name 'name' joined
		 Route.Builder bld = Route.newBuilder();
		 //bld.setId(0);
		 bld.setOrigin(origin);
		 bld.setDestination(destination);
		 bld.setPath("/join/server");
         bld.setType("join");
		 byte[] hello = name.getBytes();
		 bld.setPayload(ByteString.copyFrom(hello));

		 // blocking!
		 Route r = RouteClient.stub.request(bld.build());
         System.out.println("reply: "+ new String(r.getPayload().toByteArray()));
	 }

	public  void stopClientSession() {
		ch.shutdown();
	}

	//TODO: listen continuously for messages from server on a background thread
	public void invokeListenThread() {
		Thread thread = new Thread(){
			public void run(){
				System.out.println("Listening for messages from server...");

			}
		};
		thread.start();
	}

	public void requestIp() {
		String type = "request-ip";
		Route.Builder bld = Route.newBuilder();
		//bld.setId(i);
		bld.setOrigin("client");
		bld.setDestination(setup.getProperty("host"));
		bld.setPath(type+"/to/server");
		bld.setType(type);
		//byte[] hello = msg.getBytes();
		//bld.setPayload(ByteString.copyFrom(hello));

		// blocking!
		Route r = RouteClient.stub.request(bld.build());
		myIp = new String(r.getPayload().toByteArray());

		type = "save-client-ip";
		bld.setOrigin(myIp);
		bld.setDestination(setup.getProperty("host"));
		bld.setPath(type+"/to/server");
		bld.setType(type);
		byte[] msg = "client".getBytes();
		bld.setPayload(ByteString.copyFrom(msg));

		// blocking!
		r = RouteClient.stub.request(bld.build());
		System.out.println("reply: "+ new String(r.getPayload().toByteArray()));



	}

	public static boolean sampleBlocking(String msg, String type) {
        boolean status = false;

			Route.Builder bld = Route.newBuilder();
			//bld.setId(i);
			bld.setOrigin(origin);
			bld.setDestination(destination);
			bld.setPath(type+"/to/server");
			bld.setType(type);
			byte[] hello = msg.getBytes();
			bld.setPayload(ByteString.copyFrom(hello));

			// blocking!
			Route r = RouteClient.stub.request(bld.build());

			// TODO response handling
			String payload = new String(r.getPayload().toByteArray());
			System.out.println("reply: " +payload + ", from: " + r.getOrigin());
			if(payload.equalsIgnoreCase("success")) {
				status = true;
			}

		return status;

	}

	public static boolean deleteMessage(String msg) {
		String type = "message-delete";
		boolean result = false;
		Route.Builder bld = Route.newBuilder();
		//bld.setId(1);
		bld.setOrigin(origin);
		bld.setDestination(destination);
		bld.setPath(type+"/to/server");
		bld.setType(type);
		byte[] hello = msg.getBytes();
		bld.setPayload(ByteString.copyFrom(hello));

		// blocking!
		Route r = RouteClient.stub.request(bld.build());

		// TODO response handling
		String payload = new String(r.getPayload().toByteArray());
		System.out.println("reply: " + payload + ", from: " + r.getOrigin());
		if(payload.equalsIgnoreCase("success")) {
			result = true;
		}
		return result;
	}

	public static String getSavedMessage(String msg, String type) {
		Route.Builder bld = Route.newBuilder();
		//bld.setId(1);
		bld.setOrigin(origin);
		bld.setDestination(destination);
		bld.setPath(type+"/to/server");
		bld.setType(type);
		byte[] hello = msg.getBytes();
		bld.setPayload(ByteString.copyFrom(hello));

		// blocking!
		Route r = RouteClient.stub.request(bld.build());

		// TODO response handling
		String payload = new String(r.getPayload().toByteArray());
		System.out.println("reply: " + payload + ", from: " + r.getOrigin());
		return payload;
	}

	private static void sampleStreaming(String filename, String type) {
		if (filename == null)
			return;

		// NOTE filename is not used in the example, see server
		// implementation for details.

		System.out.println("Streaming: " + filename);

		Route.Builder bld = Route.newBuilder();
		//bld.setId(0);
		bld.setOrigin(origin);
		bld.setDestination(destination);
		bld.setPath("/save/file");

		bld.setType(type);
		byte[] fn = filename.getBytes();

		try {
			RandomAccessFile f = new RandomAccessFile(filename, "r");
			byte[] b = new byte[(int) f.length()];
			f.readFully(b);
			f.close();
			bld.setPayload(ByteString.copyFrom(b));
		} catch (IOException ie) {
			System.out.println("Unable to copy data from file..");
		}

		// we are still blocking!
		Route r = RouteClient.stub.request(bld.build());
		//while (rIter.hasNext()) {
			// process responses
			//Route r = rIter.next();
			String payload = new String(r.getPayload().toByteArray());
			System.out.println("reply: " + payload + ", from: " + r.getOrigin());
			System.out.println("reply payload: "+new String(r.getPayload().toByteArray()));
		}



	public byte[] getSavedFile(String filename, String type) {
		Route.Builder bld = Route.newBuilder();
		//bld.setId(1);
		bld.setOrigin(origin);
		bld.setPath(type+"/to/server");
		bld.setDestination(destination);
		bld.setType(type);
		byte[] hello = filename.getBytes();
		bld.setPayload(ByteString.copyFrom(hello));

		// blocking!
		Route r = RouteClient.stub.request(bld.build());

		// TODO response handling
		String payload = new String(r.getPayload().toByteArray());
		System.out.println("reply: " + payload + ", from: " + r.getOrigin());
		return  payload.getBytes();
	}

	public boolean putString(String msg) {
		String type = "message-put";
		return sampleBlocking(msg, type);
	}


	public String getString(String msg) {
		String type = "message-get";
		return getSavedMessage(msg, type);
	}


	public List<String> listMessages() {
		//List<String> stringList = new ArrayList<>();
		String type = "message-list";
		Route.Builder bld = Route.newBuilder();
		//bld.setId(1);
		bld.setOrigin(origin);
		bld.setDestination(destination);
		bld.setPath(type+"/to/server");
		bld.setType(type);
		byte[] hello = name.getBytes();
		//bld.setPayload(ByteString.copyFrom(hello));

		// blocking!
		Route r = RouteClient.stub.request(bld.build());

		// TODO response handling
		String payload = new String(r.getPayload().toByteArray());
		System.out.println("reply: " + payload + ", from: " + r.getOrigin());
		List<String> myList = new ArrayList<String>(Arrays.asList(payload.split(",")));
		return myList;
	}

	public boolean putFile(String filepath) {
		//TODO: store the file in the given path

		// 1. validate the given file path
		File fn = new File(filepath);

		// 2. send the file to server (call sendMessage())
		if (fn.exists()) {
			sampleStreaming(filepath, "file-put");
		} else {
			System.out.println("File does not exist in the given path");
			return false;
		}

		// 3. if success from server, return true
		return true;
	}

	public byte[] getFile(String filename) {
		//TODO: retrieve the file with given name
		// 1. send the file name request to server
		return getSavedFile(filename, "file-get");
		// 2. wait for response from server
		// 3. return the file content
		//return new byte[5];
	}

	public List<String> listFiles() {
		// TODO: list all the files stored
		// 1. send the list file request to server

		// 2. wait for response from server
		// 3. return the file list
		return new ArrayList<>();
	}

	public boolean deleteFile(String filename) {
		//TODO: delete the file with given name
		// 1. send the delete file name request to server
		// 2. wait for response from server
		// 3. return success on deletion of the file
		return true;
	}


}
