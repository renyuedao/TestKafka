package com.producer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gaurav.kafka.constants.IKafkaConstants;
import com.gaurav.kafka.producer.ProducerCreator;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class CSVProducer implements Runnable {

	Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	BlockingQueue<String> msgQueue = null;

	public CSVProducer(BlockingQueue<String> msgQueue) {
		this.msgQueue = msgQueue;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// System.out.println("Producer Start");
		String filepath = "C:\\test\\SampleCSVFile_556kb.csv";

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		CSVReader reader = new CSVReader(msgQueue, filepath);
		new Thread(reader).start();

		CSVProducer producer = new CSVProducer(msgQueue);
		new Thread(producer).start();

	}

	public void run() {
		runFile();
	}

	public void runFile() {
		logger.info("SetUp");
		Producer<Long, String> producer = ProducerCreator.createProducer();

		try {
			while (true) {
				String msg = msgQueue.take();
				logger.info(msg);

				ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, msg);
				try {
					RecordMetadata metadata = producer.send(record).get();

				} catch (ExecutionException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				} catch (InterruptedException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				}

				if (msg.equals("EOF")) {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		logger.info("End of Application");
	}

}
