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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer implements Runnable {

	Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	String ck = "Aty8RQgKrgaFwVRNobd6F1xd8";
	String csk = "qRAm8pJTfh56IECzgASeCtDnXWup7ZIPoo5ePkoTOYxzRdc40j";
	String tk = "782602143740076034-Uqf4AZdJJrdrQhZqEOvrg5QdKB7OGMU";
	String tsk = "syEbT8BZYnCuMJei9PPt8ZRQcPXPDR8RFqpc2xnQOjAns";
	BlockingQueue<String> msgQueue = null;

	public TwitterProducer(BlockingQueue<String> msgQueue) {
		this.msgQueue = msgQueue;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// System.out.println("Producer Start");
		// String filepath = "C:\\test\\SampleCSVFile_556kb.csv";

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		TwitterProducer producer = new TwitterProducer(msgQueue);
		new Thread(producer).start();

	}

	public void run() {
		runTwitter();
	}

	public void runTwitter() {
		logger.info("SetUp");

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		Client client = createTwitterClient(msgQueue);
		client.connect();

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);

				logger.info("5sec");

				if (msg != null) {
					Gson gson = new GsonBuilder().setPrettyPrinting().create();
					JsonParser jp = new JsonParser();
					JsonElement je = jp.parse(msg);
					String prettyJsonString = gson.toJson(je);
					logger.info(prettyJsonString);
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				client.stop();
			}
		}

		logger.info("End of Application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("Customer 360");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(ck, csk, tk, tsk);

		ClientBuilder builder = new ClientBuilder().name("yuz_test_1") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		// hosebirdClient.connect();

		return hosebirdClient;
	}

}
