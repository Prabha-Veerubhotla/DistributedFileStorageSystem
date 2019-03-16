package grpc.route.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

public class RouteClient {
	private static long clientID = 501;
	private static ManagedChannel ch;
	private static RouteServiceGrpc.RouteServiceBlockingStub stub;
	private Properties setup;
	private static String name;
	private Route.Builder bld;
	private Route r;

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
		stub = RouteServiceGrpc.newBlockingStub(ch);
		System.out.println("Client running on " + host + ": " + port);

		bld = Route.newBuilder();
	}

	 //receive message from server and respond
     public void respond() {
		 // TODO response handling
		 String payload = new String(r.getPayload().toByteArray());
		 System.out.println("reply: " + r.getId() + ", from: " + r.getOrigin() + ", payload: " + payload);
	 }

	 public void join(String name) {
		//TODO: send message to server that client with name 'name' joined
		 System.out.println("Joined successfully");
	 }

	public  void stopClientSession() {
		ch.shutdown();
	}

	//TODO: listen continuously for messages from server on a bckground thread

	public void sendMessage(String msg) {
		System.out.println("Sending message to server");
		// send messages to server
		int I = 10;
		for (int i = 0; i < I; i++) {

			bld.setId(i);
			bld.setOrigin(RouteClient.clientID);
			bld.setPath("/to/server");

			byte[] hello = msg.getBytes();
			bld.setPayload(ByteString.copyFrom(hello));

			// blocking!
			//TODO: make it asynchronous
		    r = stub.request(bld.build());
		}
	}

	public boolean putFile(String filepath) {
        //TODO: store the file in the given path
		// 1. validate the given file path
		// 2. send the file to server (call sendMessage())
		// 3. if success from server, return true
		return true;
	}

	public byte[] getFile(String filename) {
        //TODO: retrieve the file with given name
		// 1. send the file name request to server
		// 2. wait for response from server
		// 3. return the file content
		return new byte[5];
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
