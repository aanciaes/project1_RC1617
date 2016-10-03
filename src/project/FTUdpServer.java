package project;

/**
 * TftpServer Version 2016 - a very simple TFTP like server - RC FCT/UNL 2016/2017
 * 
 * Limitations:
 * 		default port is not 69;
 * 		ignores mode (always works as octet (binary));
 * 		only receives files
 **/

import static project.TftpPacketV16.MAX_TFTP_PACKET_SIZE;
import static project.TftpPacketV16.DEFAULT_TFTP_PACKET_SIZE;
import static project.TftpPacketV16.DEFAULT_BLOCK_SIZE;
import static project.TftpPacketV16.DATA_OFFSET;
import static project.TftpPacketV16.OP_ACK;
import static project.TftpPacketV16.OP_DATA;
import static project.TftpPacketV16.OP_ERROR;
import static project.TftpPacketV16.OP_WRQ;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class FTUdpServer implements Runnable {
    public  static final int DEFAULT_PORT = 10512; // my default port
    private static final String[] ACCEPTED_OPTIONS = new String[] { "blksize" };
    private static final int DEFAULT_WINDOW_SIZE = 1;
    private static final int DEFAULT_TRANSFER_TIMEOUT = 10000;

    private String filename;
    private SocketAddress cltAddr;

    private int blockSize;
    private int windowSize;
    private SortedSet<Long> window;

    FTUdpServer(int windowSize, TftpPacketV16 req, SocketAddress cltAddr) {
	this.cltAddr = cltAddr;
	this.windowSize = windowSize;
	Map<String, String> options = req.getOptions();

	if (options.containsKey("blksize"))
	    this.blockSize = Integer.valueOf(options.get("blksize"));
	else
	    this.blockSize = DEFAULT_TFTP_PACKET_SIZE - DATA_OFFSET;

	System.err.println("Using block size: "+this.blockSize);
	filename = req.getFilename();
    }

    public void run() {
	System.out.println("\nSTART!");
	System.err.println("receiving file: " + filename);
	receive();
	System.out.println("DONE!\n");
    }

 
    /*
     * Receive a file using any protocol
     */
    private void receive() {
	try {
	    window = new TreeSet<Long>();

	    DatagramSocket socket = new DatagramSocket();
	    // DatagramSocket socket = new MyDatagramSocket();

	    // Defines the timeout to end the server, in case the client
	    // stops sending data or stops sending the last ack
	    socket.setSoTimeout(DEFAULT_TRANSFER_TIMEOUT);

	    // confirms the file transfer request
	    sendAck(socket, 0L, 0L, cltAddr, " wrq ack");

	    // next block in sequence
	    long nextBlockByte = 1L; // we wait for the first byte
	    boolean receivedLastBlock = false;
	    long fileSizePlusOne = 0L; // or the last ack cumulative sequence number

	    RandomAccessFile raf = new RandomAccessFile(filename + ".bak", "rw");

	    while (!receivedLastBlock || window.size() > 0) {
		// get a packet
		byte[] buffer = new byte[MAX_TFTP_PACKET_SIZE];
		DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		socket.receive(datagram);
		TftpPacketV16 pkt = new TftpPacketV16(datagram.getData(), datagram.getLength());
		System.err.println("received: " + pkt);
		
		switch (pkt.getOpcode()) {
		case OP_DATA:
		    // Is this the last block of the file ?
		    if ( pkt.getBlockData().length < blockSize ) {
			// as packet may arrive out of order, don't foget it
			if (!receivedLastBlock) receivedLastBlock = true;
			// record the cumulative sequence number of the last block
			// which is equal to the size of the file + 1
			fileSizePlusOne = pkt.getSeqN()+pkt.getBlockData().length;
		    }
		    // does the block is out of the window ?
		    if ( pkt.getSeqN() < nextBlockByte || pkt.getSeqN() > nextBlockByte + windowSize * blockSize ) {
			System.err.println("received packet out of window, ignoring...");
			sendAck(socket, nextBlockByte, -1, cltAddr, "ignored");
			continue; // get next packet
		    }
		    // is this block a duplicate ? if not save it at the proper offset
		    if ( ! window.contains(pkt.getSeqN())) {
			window.add(pkt.getSeqN());
			byte[] data = pkt.getBlockData();
			raf.seek(pkt.getSeqN() - 1L);
			raf.write(data);
		    }
		    System.err.println( "window: "+window.stream().map( v -> v / blockSize).collect( Collectors.toList()));
		    // before sending the ack, try to slide window
		    while (window.size() > 0 && window.first() == nextBlockByte) {
			window.remove(window.first());
			nextBlockByte += blockSize;
		    }
		    // If we are acking the last block of the file for the last time,
		    // as this block is smaller then other blocks, the value of the
		    // cumulative ack is special and
		    // nextBlockByte should be equal to the last cumulative sequence number
		    System.err.println( "window: "+window.stream().map( v -> v / blockSize).collect( Collectors.toList()));
		    if ( receivedLastBlock && window.size() == 0 ) {
			nextBlockByte = fileSizePlusOne;
			System.err.println("sending last ack");
		    }
		    sendAck(socket, nextBlockByte, pkt.getSeqN(), cltAddr);
		    break;
		    
		case OP_WRQ:
		    sendAck(socket, 0L, 0L, cltAddr, " wrq ack");
		    break;
		    
		default:
		    throw new RuntimeException("Error receiving file." + filename + "/" + pkt.getOpcode());
		} // end switch 
	    } // end while
	    raf.close();
	    
	    // we still resend the last ack for a while since it can be lost
	    try {
		for(;;) { // until interrupted
		    byte[] buffer = new byte[MAX_TFTP_PACKET_SIZE];
		    DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		    socket.receive(datagram);
		    sendAck(socket, fileSizePlusOne, nextBlockByte, cltAddr, "re sending last ack");
		}
	    } catch (SocketTimeoutException x) {
		System.err.printf("End of reception ...\n");
	    }

	} catch (SocketTimeoutException x) {
	    System.err.printf("Interrupted transfer\n");
	} catch (Exception x) {
	    System.err.println("Receive failed: " + x.getMessage());
	}
    }

    /*
     * Prepare and send a TftpPacketV16 ACK
     */
    private static void sendAck(DatagramSocket s, long cumulativeSeqN, long currentSeqN, SocketAddress dst, String... debugMessages) throws IOException {
	TftpPacketV16 ack = new TftpPacketV16().putShort(OP_ACK).putLong(cumulativeSeqN).putLong(currentSeqN);
	s.send(new DatagramPacket(ack.getPacketData(), ack.getLength(), dst));
	if (debugMessages.length > 0)
	    System.err.printf("sent: %s %s\n", ack, debugMessages[0]);
	else
	    System.err.printf("sent: %s \n", ack);

    }

    public static void main(String[] args) throws Exception {
	// MyDatagramSocket.init(1, 1);
	int port = DEFAULT_PORT;
	int windowSize = DEFAULT_WINDOW_SIZE;
	try {
	    switch (args.length) {
	    case 1:
		windowSize = Integer.valueOf(args[0]);
	    case 0:
		break;
	    default:

		throw new Exception();
	    }
	} catch (Exception x ) {
	    System.out.println("usage: java TftpServer [window size]");
	    System.exit(0);
	}
	if ( windowSize < 1 ) windowSize = 1;

	// create and bind socket to port for receiving client requests
	// DatagramSocket mainSocket = new MyDatagramSocket(port);
	DatagramSocket mainSocket = new DatagramSocket(port);
	System.out.println("\nNew tftp server started at local port: " + mainSocket.getLocalPort()+" window size: "+windowSize+"\n");

	for (;;) { // loop until interrupted
	    try {
		// receives request from clients
		byte[] buffer = new byte[MAX_TFTP_PACKET_SIZE];
		DatagramPacket msg = new DatagramPacket(buffer, buffer.length);
		mainSocket.receive(msg);

		// look at datagram as a TFTP packet
		TftpPacketV16 req = new TftpPacketV16(msg.getData(), msg.getLength());
		switch (req.getOpcode()) {
		case OP_WRQ: // Write Request
		    System.err.println("write request: " + req);
		    // Launch a dedicated thread to handle the client request
		    new Thread(new FTUdpServer(windowSize, req, msg.getSocketAddress())).start();
		    break;
		default: // unexpected packet op code!
		    System.err.printf("Unknown packet opcode %d ignored\n", req.getOpcode());
		    sendError(mainSocket, 0, "Unknown request type..." + req.getOpcode(), msg.getSocketAddress());
		}
	    } catch (Exception x) {
		x.printStackTrace();
	    }
	}
    }

    /*
     * Sends an error packet
     */
    private static void sendError(DatagramSocket s, int err, String str, SocketAddress dstAddr) throws IOException {
	TftpPacketV16 pkt = new TftpPacketV16().putShort(OP_ERROR).putShort(err).putString(str).putByte(0);
	s.send(new DatagramPacket(pkt.getPacketData(), pkt.getLength(), dstAddr));
    }

}
