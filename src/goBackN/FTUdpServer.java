package goBackN;

/**
 * TftpServer Version 2016 - a very simple TFTP like server - RC FCT/UNL 2016/2017
 * 
 * Limitations:
 * default port is not 69;
 * ignores mode (always works as octet (binary));
 * only receives files
 **/

import static goBackN.TftpPacketV16.MAX_TFTP_PACKET_SIZE;
import static goBackN.TftpPacketV16.DEFAULT_TFTP_PACKET_SIZE;
import static goBackN.TftpPacketV16.DATA_OFFSET;
import static goBackN.TftpPacketV16.OP_ACK;
import static goBackN.TftpPacketV16.OP_DATA;
import static goBackN.TftpPacketV16.OP_ERROR;
import static goBackN.TftpPacketV16.OP_WRQ;

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
    public static final String SRV = "SRV: ";

    public static final int DEFAULT_PORT = 10512; // my default port
    private static final String[] ACCEPTED_OPTIONS = new String[] { "blksize" };
    private static final int DEFAULT_WINDOW_SIZE = 1; // by default stop and
    // wait
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

	System.err.println(SRV + "Using block size: " + this.blockSize);
	filename = req.getFilename();
    }

    public void run() {
	System.out.println(SRV + "START!");
	System.err.println(SRV + "receiving file: " + filename);
	receive();
    }

    /*
     * Receive a file using any protocol
     */
    private void receive() {
	window = new TreeSet<Long>();

	long fileSizePlusOne = -1L; 
	
	try(RandomAccessFile raf = new RandomAccessFile(filename + ".bak", "rw")) {

		DatagramSocket socket = new DatagramSocket();

		// Defines the timeout to end the server in case the client
		// stops sending data 
		socket.setSoTimeout(DEFAULT_TRANSFER_TIMEOUT);

		// confirms the file transfer request
		sendAck(socket, 0L, 0L, cltAddr, " wrq ack");

		// next block in sequence
		long nextBlockByte = 1L; // we wait for the first byte
		
		byte[] buffer = new byte[MAX_TFTP_PACKET_SIZE];
		
		for(;;) {
		    
		    // get a packet
		    DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
		    socket.receive(datagram);
		    
		    TftpPacketV16 pkt = new TftpPacketV16(datagram.getData(), datagram.getLength());
		    System.err.println(SRV + "received: " + pkt);

		    switch (pkt.getOpcode()) {
		    case OP_DATA:
			
			// ignore duplicate packets that already slided out of the window...
			if (pkt.getSeqN() < nextBlockByte ) {
			    System.err.println(SRV + "received duplicate packet, ignoring...");
			    sendAck(socket, nextBlockByte, pkt.getSeqN(), cltAddr, "ignored");
			    continue; // get next packet
			}
			
			// acknowledge but discard packets that are outside the window...
			if (pkt.getSeqN() > nextBlockByte + windowSize * blockSize) {
			    System.err.println(SRV + "received packet out of window, ignoring...");
			    sendAck(socket, nextBlockByte, -1L, cltAddr, "ignored");
			    continue; // get next packet
			}

			// write the payload to the file, unless the packet is a duplicate...
			if (! window.contains(pkt.getSeqN()) ) {

			    window.add(pkt.getSeqN());
			    
			    raf.seek(pkt.getSeqN() - 1L);
			    byte[] payload = pkt.getBlockData();
			    raf.write(payload);
			    
			    // is this the last block? 
			    if (payload.length < blockSize) 
				fileSizePlusOne = pkt.getSeqN() + payload.length;
			}
			
			System.err.println( SRV + "win: " + window.stream().map(v -> v/blockSize).collect(Collectors.toList()));
			
			// try to slide window
			while (window.size() > 0 && window.first() == nextBlockByte) {
			    window.remove(window.first());
			    nextBlockByte += blockSize;
			    
			    // avoid exceeding file size
			    if( fileSizePlusOne >= 0L && nextBlockByte > fileSizePlusOne)
				nextBlockByte = fileSizePlusOne;
			}
			
			System.err.println( SRV + "win: " + window.stream().map(v -> v/blockSize).collect(Collectors.toList()));

			// ack the packet
			sendAck(socket, nextBlockByte, pkt.getSeqN(), cltAddr);
			break;

		    case OP_WRQ:
			sendAck(socket, 0L, 0L, cltAddr, " wrq ack");
			continue;

		    default:
			throw new RuntimeException("Error receiving file." + filename + "/" + pkt.getOpcode());
		    } // end switch
		} 
	    } catch (SocketTimeoutException x) {
	    //System.err.printf("End waiting for client\n");
	} catch (Exception x) {
	    System.err.println("Receive failed: " + x.getMessage());
	}
	
	if( fileSizePlusOne > 0 && window.isEmpty() ) {
	    System.err.println( SRV + " Done!!!");
	} else {
	    System.err.println( SRV + " Error! [Timeout/Incomplete File!!!]");
	}
    }

    /*
     * Prepare and send a TftpPacketV16 ACK
     */
    private static void sendAck(DatagramSocket s, long cumulativeSeqN, long currentSeqN, SocketAddress dst,
				String... debugMessages) throws IOException {
	TftpPacketV16 ack = new TftpPacketV16().putShort(OP_ACK).putLong(cumulativeSeqN).putLong(currentSeqN);
	s.send(new DatagramPacket(ack.getPacketData(), ack.getLength(), dst));
	if (debugMessages.length > 0)
	    System.err.printf(SRV+"sent: %s %s\n", ack, debugMessages[0]);
	else
	    System.err.printf(SRV+"sent: %s \n", ack);

    }

    public static void main(String[] args) throws Exception {
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
	} catch (Exception x) {
	    System.out.println(SRV+"usage: java TftpServer [window size]");
	    System.exit(0);
	}
	if (windowSize < 1)
	    windowSize = 1;

	// create and bind socket to port for receiving client requests
	// DatagramSocket mainSocket = new MyDatagramSocket(port);
	DatagramSocket mainSocket = new DatagramSocket(port);
	System.out.println(SRV+"New tftp server started at local port: " + mainSocket.getLocalPort() + " window size: "
			   + windowSize + "\n");

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
		    System.err.printf(SRV+"Unknown packet opcode %d ignored\n", req.getOpcode());
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


