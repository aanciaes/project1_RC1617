package goBackN;

import static goBackN.TftpPacketV16.DEFAULT_BLOCK_SIZE;
import static goBackN.TftpPacketV16.MAX_TFTP_PACKET_SIZE;
import static goBackN.TftpPacketV16.OP_ACK;
import static goBackN.TftpPacketV16.OP_DATA;
import static goBackN.TftpPacketV16.OP_WRQ;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class FTUdpClient {
	static final int DEFAULT_TIMEOUT = 1000;
	static final int DEFAULT_MAX_RETRIES = 7;
	static int BlockSize = DEFAULT_BLOCK_SIZE;
	static int Timeout = DEFAULT_TIMEOUT;
	private static int staticWindowSize;

	private int WindowSize; 
	private String filename;
	private Stats stats;
	private DatagramSocket socket;
	private BlockingQueue<TftpPacketV16> receiverQueue;
	volatile private SocketAddress srvAddress;

	//Saves all packets inside the window that have already been sent
	private List<Long> sentPackets;
	private List<Long> sentPacketsTime;

	//Represents the client side window
	private SortedMap<Long, TftpPacketV16> window;
	long byteCount = 1; // block byte count starts at 1

	private int windowUpdateCounter = 1; //Ajustable window size

	//ajustable Timeout
	private long allPropagationTime;
	private long numACKS;

	FTUdpClient(String filename, SocketAddress srvAddress) {
		this.filename = filename;
		this.srvAddress = srvAddress;
		window = new TreeMap <Long, TftpPacketV16>();
		WindowSize = staticWindowSize;

		sentPackets = new ArrayList<Long>(WindowSize);//Array list with same size as the window
		sentPacketsTime = new ArrayList<Long>(WindowSize);
	}

	void sendFile() {
		try {

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
			send (wrr, 0L);
			try {

				FileInputStream f = new FileInputStream(filename);

				//Initial free space is equal to window size -- No packets in the window initially
				int freeSpace = WindowSize;

				while (f.available()>0 || !window.isEmpty()) { //While there is file to be read or window is not empty

					//Since fillWindow fills all the freeSlots, it just sets freeSpace to 0
					freeSpace = fillWindow(freeSpace, f);

					sendDataFromWindow(); //Send all data from window

					TftpPacketV16 ack = receiverQueue.poll(Timeout, TimeUnit.MILLISECONDS);
					System.err.println(">>>> got: " + ack);
					if (ack != null)
						if (ack.getOpcode() == OP_ACK)
							if(ack.getCurrentSeqN()!=-1){
								if (window.containsKey(ack.getCumulativeSeqN())) { 	//Acknowledge received is inside the window

									//ajustable Timeout
									numACKS++;
									for(int i = 0; i<sentPackets.size();i++){
										if(sentPackets.get(i) == ack.getCumulativeSeqN()){
											ajustTimeout(sentPacketsTime.get(i));
											break;
										}
									} //Ajustable Timeout

									//slide window
									freeSpace = handleSliding(ack.getCumulativeSeqN());
									windowUpdateCounter++;

									if(windowUpdate() ){	//If there was an update on the window size
										freeSpace++;
										List<Long> tmp = sentPackets;

										//Resize Lists
										sentPackets = new ArrayList<Long>(WindowSize);
										sentPackets.addAll(tmp);
										List<Long> tmp2 = sentPacketsTime;
										sentPacketsTime = new ArrayList<Long>(WindowSize);
										sentPacketsTime.addAll(tmp2);
									}

								} else {
									//wrong acknowledge
									windowUpdateCounter = 1; //Error --> Reset Window counter
									System.err.println("wrong ack ignored, block= " + ack.getSeqN());
								}
							}else{
								sentPackets.clear(); //Clear all sent packets
								System.out.println("Packet Lost");

							}
						else {
							windowUpdateCounter = 1;//Error --> Reset Window counter
							System.err.println("error +++ (unexpected packet)");
						}
					else {
						//Timeout expired. Clear all data from sent packets to send them again
						windowUpdateCounter = 1;
						System.err.println("timeout...");
						//Resize window
						WindowSize = staticWindowSize;
						freeSpace=0;
						sentPackets = new ArrayList<Long>(WindowSize);

						//stats
						sentPacketsTime = new ArrayList<Long>(WindowSize);
					}
				}
				f.close();

			} catch (Exception e) {
				System.err.println("failed with error \n" + e.getMessage());
				e.printStackTrace();
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

	/**
	 * Updates the window size
	 * @return true if the window size was updated
	 */
	private boolean windowUpdate(){

		if(WindowSize >= 29){
			return false;
		}
		if(windowUpdateCounter >= 3){

			WindowSize++;
			return true;
		}
		return false;
	}

	/**
	 * Handles the slide of the window after receiving an acknowledge
	 * @param currentSeqN The acknowledge received
	 * @return Free spaces in the window after sliding
	 */
	private int handleSliding(long seqNumber) {

		int x = 0;
		int freeSpaces = 0;
		while(x < sentPackets.size()){

			if(seqNumber >= sentPackets.get(x)){ 
				//acknowledge received, packet can be deleted from window
				stats.newPacketSent(window.get(sentPackets.get(x)).getBlockData().length);
				window.remove(sentPackets.get(x));
				sentPackets.remove(x);
				freeSpaces++;

				//stats
				stats.newTimeoutMeasure(System.currentTimeMillis() - sentPacketsTime.get(x));
				sentPacketsTime.remove(x);

			}
			x++;
		}
		//If after the window resizing, the packets are already loaded into memory
		if(window.size() > WindowSize){
			return 0;
		}
		return freeSpaces;
	}

	/**
	 * Reads data from the file and fills the free spaces on the window with data from the file
	 * @param freeSpace Number of free spaces in available on the window
	 * @param file File to be read and transfered to the server
	 * @return The number of free spaces left in the window after filling the window. Normally it returns 0, cause it fills all free spaces
	 */
	private int fillWindow(int freeSpace, FileInputStream file) {

		int i = 0;
		int n;
		int free = freeSpace;
		byte[] buffer = new byte[BlockSize];
		try {
			while (i < freeSpace) {
				//Reads data from the file into the buffer
				if ((n = file.read(buffer)) > 0 ) {
					//Creates a new packet and stores it in the window
					TftpPacketV16 pkt = new TftpPacketV16().putShort(OP_DATA).putLong(byteCount).putBytes(buffer, n);
					byteCount += n;
					window.put(byteCount, pkt);

					free--;
				}
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return free;
	}

	/**
	 * Sends packets that have not yet been sent in this current timeout
	 * @throws IOException
	 */
	public void sendDataFromWindow() throws IOException{
		int counter = 0;
		for(Long seq : window.keySet()) {
			TftpPacketV16 pkt = window.get(seq);
			counter++;
			if(!sentPackets.contains(seq)){

				System.err.println("sending: " + (seq - pkt.getPacketData().length) + " expecting:" + (seq));
				socket.send(new DatagramPacket(pkt.getPacketData(), pkt.getLength(), srvAddress));
				sentPackets.add(seq);

				//Stats
				sentPacketsTime.add(System.currentTimeMillis());


			}
			if(counter == WindowSize){
				break;
			}
		}
	}

	/**
	 * Adjusts the timeout
	 * @param time time of the received acknowledge
	 */
	public void ajustTimeout (long time){

		allPropagationTime += (System.currentTimeMillis() - time);

		Timeout = (int)(allPropagationTime/numACKS + 20);
		System.out.println("Timeout = " + Timeout);

	}


	/*
	 * Send a block to the server, repeating until the expected ACK is received,
	 * or the number of allowed retries is exceeded.
	 */
	void send (TftpPacketV16 blk, long expectedACK) throws Exception {

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

		void newPacketSent(long x) {
			totalPackets++;
			totalBytes += x;
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
			System.out.println("Average Timeout: " + Timeout);
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
				staticWindowSize = Integer.parseInt(args[4]);
			case 4:
				staticWindowSize=5;
				Timeout = Integer.valueOf(args[3]);
			case 3:
				staticWindowSize=5;
				Timeout=1000;
				BlockSize = Integer.valueOf(args[2]);
			case 2:
				staticWindowSize = 5;
				Timeout=1000;
				BlockSize=512;
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

