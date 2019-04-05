package grpc.route.server;

import io.grpc.ManagedChannel;

public class Node_ip_channel {
    String ipAddress;
    ManagedChannel channel;

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void setChannel(ManagedChannel channel) {
        this.channel = channel;
    }
}
