package goBackN;

import static goBackN.TftpPacketV16.MAX_TFTP_PACKET_SIZE;
import static goBackN.TftpPacketV16.DEFAULT_BLOCK_SIZE;
import static goBackN.TftpPacketV16.OP_ACK;
import static goBackN.TftpPacketV16.OP_DATA;
import static goBackN.TftpPacketV16.OP_WRQ;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class FTUdpClient {
    static final int DEFAULT_TIMEOUT = 1000;
    static final int DEFAULT_MAX_RETRIES = 7;
    static int WindowSize = 1; // This cliente is a stop and wait one
    static int BlockSize = DEFAULT_BLOCK_SIZE;
    static int Timeout = DEFAULT_TIMEOUT;

    private String filename;
    private Stats stats;
    private DatagramSocket socket;
    private BlockingQueue<TftpPacketV16> receiverQueue;
    volatile private SocketAddress srvAddress;

    FTUdpClient(String filename, SocketAddress srvAddress) {
	this.filename = filename;
	this.srvAddress = srvAddress;
    }

    void sendFile() {
	try {

	    // socket = new MyDatagramSocket();
	    socket = new DatagramSocket();

	    // create producer/consumer queue for ACKs
	    receiverQueue = new ArrayBlockingQueue<>(1);
	    // for statistics
	    stats = new Stats();

	    // start a receiver process to feed the queue
	    new Thread(() -> {
		    try {
			for (;;) {
			    byte[] buffer = new byte[MAX_TFTP_PACKET_SIZE];
			    DatagramPacket msg = new DatagramPacket(buffer, buffer.length);
			    socket.receive(msg);
			    // update server address (it may change due to WRQ
			    // coming from a different port
			    srvAddress = msg.getSocketAddress();
			    System.err.println("CLT: " + new TftpPacketV16(msg.getData(), msg.getLength()));
			    // make the packet available to sender process
			    TftpPacketV16 pkt = new TftpPacketV16(msg.getData(), msg.getLength());
			    receiverQueue.put(pkt);
			}
		    } catch (Exception e) {
		    }
	    }).start();

	    System.out.println("\nSending file: \"" + filename + "\" to server: " + srvAddress + " from local port:"
			       + socket.getLocalPort() + "\n");
	    TftpPacketV16 wrr = new TftpPacketV16().putShort(OP_WRQ).putString(filename).putByte(0).putString("octet")
		.putByte(0).putString("blksize").putByte(0).putString(Integer.toString(BlockSize)).putByte(0);
	    sendRetry(wrr, 0L, DEFAULT_MAX_RETRIES);
	    try {

		FileInputStream f = new FileInputStream(filename);
		long byteCount = 1; // block byte count starts at 1
		// read and send blocks
		int n;
		byte[] buffer = new byte[BlockSize];
		while ((n = f.read(buffer)) > 0) {
		    TftpPacketV16 pkt = new TftpPacketV16().putShort(OP_DATA).putLong(byteCount).putBytes(buffer, n);
		    byteCount += n;
		    stats.newPacketSent(n);
		    sendRetry(pkt, byteCount, DEFAULT_MAX_RETRIES);
		}
		f.close();

	    } catch (Exception e) {
		System.err.println("failed with error \n" + e.getMessage());
		System.exit(0);
	    }
	    socket.close();
	    System.out.println("Done...");
	} catch (Exception x) {
	    x.printStackTrace();
	    System.exit(0);
	}
	stats.printReport();
    }

    /*
     * Send a block to the server, repeating until the expected ACK is received,
     * or the number of allowed retries is exceeded.
     */
    void sendRetry(TftpPacketV16 blk, long expectedACK, int retries) throws Exception {
	for (int i = 0; i < retries; i++) {

            // Thread.sleep(1000); if you want to transmit slowly...
	    System.err.println("sending: " + blk);
	    long sendTime = System.currentTimeMillis();
	    socket.send(new DatagramPacket(blk.getPacketData(), blk.getLength(), srvAddress));
	    
	    long remaining;
	    while((remaining = (sendTime + Timeout) - System.currentTimeMillis()) > 0) {
		TftpPacketV16 ack = receiverQueue.poll(remaining, TimeUnit.MILLISECONDS);
		if (ack != null)
		    if (ack.getOpcode() == OP_ACK)
			if (expectedACK == ack.getCumulativeSeqN()) {
			    stats.newTimeoutMeasure(System.currentTimeMillis() - sendTime);
			    System.err.println("got expected ack: " + expectedACK);
			    return;
			} else {
			    System.err.println("got wrong ack");
			    continue;
			}
		    else {
			System.err.println("got unexpected packet (error)");
			continue;
		    }
		else
		    System.err.println("timeout...");
	    } 
	}
	throw new IOException("too many retries");
    }

    class Stats {
	private long totalRtt = 0;
	private int timesMeasured = 0;
	private int window = 1;
	private int totalPackets = 0;
	private int totalBytes = 0;
	private long startTime = 0L;;

	Stats() {
	    startTime = System.currentTimeMillis();
	}

	void newPacketSent(int n) {
	    totalPackets++;
	    totalBytes += n;
	}

	void newTimeoutMeasure(long t) {
	    timesMeasured++;
	    totalRtt += t;
	}

	void printReport() {
	    // compute time spent receiving bytes
	    int milliSeconds = (int) (System.currentTimeMillis() - startTime);
	    float speed = (float) (totalBytes * 8.0 / milliSeconds / 1000); // M
	    // bps
	    float averageRtt = (float) totalRtt / timesMeasured;
	    System.out.println("\nTransfer stats:");
	    System.out.println("\nFile size:\t\t\t" + totalBytes);
	    System.out.println("Packets sent:\t\t\t" + totalPackets);
	    System.out.printf("End-to-end transfer time:\t%.3f s\n", (float) milliSeconds / 1000);
	    System.out.printf("End-to-end transfer speed:\t%.3f M bps\n", speed);
	    System.out.printf("Average rtt:\t\t\t%.3f ms\n", averageRtt);
	    System.out.printf("Sending window size:\t\t%d packet(s)\n\n", window);
	}
    }

    public static void main(String[] args) throws Exception {
	// MyDatagramSocket.init(1, 1);
	try {
	    switch (args.length) {
	    case 5:
		// By the moment this parameter is ignored and the client
		// WindowSize
		// is always equal to 1 (stop and wait)
		// WindowSize = Integer.parseInt(args[4]);
	    case 4:
		Timeout = Integer.valueOf(args[3]);
	    case 3:
		BlockSize = Integer.valueOf(args[2]);
	    case 2:
		break;
	    default:
		throw new Exception("bad parameters");
	    }
	} catch (Exception x) {
	    System.out.printf("usage: java FTUdpClient filename servidor [blocksize [ timeout [ windowsize ]]]\n");
	    System.exit(0);
	}
	String filename = args[0];
	String server = args[1];
	SocketAddress srvAddr = new InetSocketAddress(server, FTUdpServer.DEFAULT_PORT);
	new FTUdpClient(filename, srvAddr).sendFile();
    }

} // FTUdpClient
