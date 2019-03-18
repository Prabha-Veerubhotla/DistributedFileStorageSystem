package grpc.route.client;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

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
	private static int port = 2345;

	public static void main(String[] args) {
		String ip = args[0];
		ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, RouteClient.port).usePlaintext(true).build();

		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

		int I = 10;
		for (int i = 0; i < I; i++) {
			Route.Builder bld = Route.newBuilder();
			bld.setId(i);
			bld.setOrigin(RouteClient.clientID);
			bld.setPath("/to/somewhere");

			byte[] hello = "hello".getBytes();
			bld.setPayload(ByteString.copyFrom(hello));

			// blocking!
			Route r = stub.request(bld.build());

			// TODO response handling
			String payload = new String(r.getPayload().toByteArray());
			System.out.println("reply: " + r.getId() + ", from: " + r.getOrigin() + ", payload: " + payload);
		}

		ch.shutdown();
	}
}
