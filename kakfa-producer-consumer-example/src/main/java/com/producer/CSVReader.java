package com.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class CSVReader implements Runnable {

	protected BlockingQueue<String> blockingQueue = null;
	protected String filepath = null;

	public CSVReader(BlockingQueue<String> blockingQueue, String filepath) {
		this.blockingQueue = blockingQueue;
		this.filepath = filepath;
	}

	@Override
	public void run() {
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(new File(this.filepath)));
			String buffer = null;
			while ((buffer = br.readLine()) != null) {
				blockingQueue.put(buffer);
			}
			blockingQueue.put("EOF");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}
