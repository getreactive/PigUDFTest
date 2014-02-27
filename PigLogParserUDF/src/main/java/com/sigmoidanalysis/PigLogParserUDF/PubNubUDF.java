package com.sigmoidanalysis.PigLogParserUDF;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class PubNubUDF extends EvalFunc<Tuple> {

	public HashMap<String, String> myMap = new HashMap<String, String>();
	public HashMap<String, String> tempMap = new HashMap<String, String>();
	TupleFactory tf = TupleFactory.getInstance();

	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		if (input == null || input.size() < 1 || input.isNull(0)) {
			return null;
		}
		Tuple output_tpl = tf.newTuple();

		try {

			// String other_data = "";
			// String subkey = "";
			// String client_ip = "";
			HashMap<String, String> hmap = new HashMap<String, String>();
			// int check = 0;
			String input_str = (String) input.get(0);
			System.out.println("######" + input.get(0) + "######");
			// String temp_str1 = input_str.replaceAll("\"", "\"");
			// hmap.putAll(parse_line(temp_str1));
			hmap.putAll(parse_line(input_str));
			/*
			 * Iterator<String> keySetIterator = hmap.keySet().iterator();
			 * while(keySetIterator.hasNext()) { String key =
			 * keySetIterator.next(); if(key.equals("sub_key")) { subkey =
			 * hmap.get(key).toString(); } else if(key.equals("client_ip")) {
			 * client_ip = hmap.get(key).toString(); } else { if(check == 0) {
			 * other_data += hmap.get(key).toString(); check++; } else {
			 * other_data += " " + hmap.get(key).toString(); } }//end of outer
			 * else // System.out.println("key: " + key + " value: " +
			 * hmap.get(key)); }//end of while
			 */

			// String tuple_str= "Yohohoho";
			// String str2 = "sunga";
			output_tpl.append(hmap.get("sub_key"));
			output_tpl.append(hmap.get("client_ip"));
			output_tpl.append(input_str.toString());
			return output_tpl;

		} catch (Exception e1) {
			return null;
			// throw new IOException("Caught exception processing input row ",
			// e);
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();

			tupleSchema
					.add(new Schema.FieldSchema("field1", DataType.CHARARRAY));
			tupleSchema
					.add(new Schema.FieldSchema("field2", DataType.CHARARRAY));
			tupleSchema
					.add(new Schema.FieldSchema("field3", DataType.CHARARRAY));
			
			return tupleSchema;
		} catch (Exception e) {
			// Safely return nothing
			return null;
		}
	}

	public static Pattern match = Pattern
			.compile("([\\d\\.]+)\\s\\-\\s\\-\\s\\"
					+ "[([^\\]]+)\\]\\s\""
					+ "([^\"]*)\"\\s"
					+ "(\\d+)\\s"
					+ "(\\d+)\\s\""
					+ "([^\"]+)?\"\\s\""
					+ "([^\"]+)?\"\\s?"
					+ "(([\\d\\.]+)|\\-)?(,\\s([\\d\\.]+|\\-))*"
					// optional new stuff follows
					+ "(\\s(((([\\d\\.]+)|\\-)(,\\s([\\d\\.]+|\\-))*)|\\-)\\s"
					+ "(([\\d\\.]+)|\\-)\\s" + "(([\\d\\.]+)|\\-)\\s"
					+ "(([\\d\\.]+)|\\-)\\s" + "(([\\d\\.]+)|\\-)\\s"
					+ "(([\\d\\.]+)|\\-)\\s"
					+ "(([0-9a-zA-Z\\-\\\\.]+)|\\-)\\s" + "([sl\\-]+)\\s"
					+ "(([\\d\\.]+)|\\-)"
					// #timetoken added later
					+ "(\\s(([\\d]+)|\\-))?)?" + "(\\s([\\d\\.]+)|\\s)"
					+ "(\\s([^\\s]+)|\\s)?" + "(\\s([\\d\\.])|\\s)?");

	public static Pattern origin_matcher = Pattern.compile("http://[^/]+");

	public static Pattern subscribe_matcher = Pattern
			.compile("GET /subscribe/([^/\\ ]+)/([^/\\ ]+)/([^/\\ ]+)/([^/\\ \\?]+)/?(\\?[^/]*)?\\ HTTP");

	public static Pattern publish_matcher = Pattern
			.compile("GET\\ /publish/([^/\\ ]+)/" + "([^/\\ ]+)/([^/\\ ]+)/"
					+ "([^/]+)/([^/\\?]+)/" + "([^\\?]+)/?(\\?[^/]*)?\\ HTTP");

	public static Pattern history_matcher = Pattern
			.compile("GET /history/([^/\\ ]+)/([^/\\ ]+)/([^/\\ ]+)/([^/\\ \\?]+)/?(\\?[^\\?/\\ ]*)?\\ HTTP");

	public static Pattern history_matcher_v2 = Pattern
			.compile("GET\\ /v2/history/sub-key/"
					+ "([^/\\ ]+)/channel/([^/\\ \\?]+)/?(\\?[^\\?/\\ ]*)?\\ HTTP");

	public static Pattern presence_matcher = Pattern
			.compile("GET\\ /v2/presence/sub[-_]+key/([^/\\ ]+)/channel/([^/\\ \\?]+)/?([^/\\ \\?]+)?/?(\\?[^\\?/\\ ]*)?\\ HTTP");

	public static Pattern hex_quote = Pattern.compile("[0-9a-fA-F]{2}");

	/******************************************** publish log parser *****************************************/
	public HashMap<String, String> parse_publish(String request_uri,
			HashMap<String, String> other_tempMap) {
		Matcher m = publish_matcher.matcher(request_uri);
		tempMap.put("request_type", "publish");
		String message = "";
		String channel = "";
		if (m.find()) {
			try {
				message = URLDecoder.decode(m.group(6), "UTF-8");
				channel = URLDecoder.decode(m.group(4), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

			tempMap.put("pub_key", m.group(1));
			tempMap.put("sub_key", m.group(2));
			tempMap.put("signature", m.group(3));
			tempMap.put("message_size", String.valueOf(message.length()));
			tempMap.put("messase", message);
			tempMap.put("channel", channel);
			String queryString = m.group(7);

			if (queryString != null) {
				String pair[] = queryString.replace("?", "").split("&");
				for (int i = 0; i < pair.length; i++) {
					String keyValue[] = pair[i].split("=");
					tempMap.put(keyValue[0], keyValue[1]);
				}
			}
		}
		return tempMap;
	}

	/****************************** subscribe log parser **********************************/

	public HashMap<String, String> parse_subscribe(String request_uri,
			HashMap<String, String> other_tempMap) {
		tempMap.put("request_type", "subscribe");
		if (other_tempMap.get("byte_sent").equals("11")
				|| other_tempMap.get("byte_sent").equals("12")
				|| other_tempMap.get("num_messages").equals("0")) {
			tempMap.put("subscribe_timeout", "True");
		}
		Matcher m = subscribe_matcher.matcher(request_uri);
		String channel = "";
		if (m.find()) {
			try {
				channel = URLDecoder.decode(m.group(2), "utf-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace(); // To change body of catch statement use
										// File | Settings | File Templates.
			}
			tempMap.put("sub_key", m.group(1));
			tempMap.put("channel", channel);
			tempMap.put("timetoken", m.group(4));
			String queryString = m.group(5);
			if (queryString != null) {
				String pair[] = queryString.replace("?", "").split("&");
				for (int i = 0; i < pair.length; i++) {
					String keyValue[] = pair[i].split("=");
					tempMap.put(keyValue[0], keyValue[1]);
				}
			}

		}
		return tempMap;
	}

	/********************************** unsubscribe log parser **********************************/

	public HashMap<String, String> parse_unsubscribe(String request_uri,
			HashMap<String, String> other_tempMap) {
		tempMap.put("request_type", "unsubscribe");
		Matcher m = subscribe_matcher.matcher(request_uri);
		String Channel;
		if (m.find()) {
			try {
				Channel = URLDecoder.decode(m.group(2), "utf-8");
				tempMap.put("sub_key", m.group(1));
				tempMap.put("channel", Channel);
				tempMap.put("timetoken", m.group(4));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace(); // To change body of catch statement use
										// File | Settings | File Templates.
			}
		}

		return tempMap;
	}

	/********************************** history logs parser **************************************/

	public HashMap<String, String> parse_history(String request_uri,
			HashMap<String, String> other_tempMap) {

		HashMap<String, String> tempMap = new HashMap<String, String>();
		tempMap.put("request_type", "history");
		Matcher m;
		String queryString = "";
		if (request_uri.startsWith("GET /history")) {
			m = history_matcher.matcher(request_uri);
			if (m.find()) {
				tempMap.put("sub_key", m.group(1));
				queryString = m.group(5);
			}
		} else {
			m = history_matcher_v2.matcher(request_uri);
			if (m.find())
				queryString = m.group(3);
		}
		if (m.find()) {
			try {
				tempMap.put("channel", URLDecoder.decode(m.group(1), "utf-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace(); // To change body of catch statement use
										// File | Settings | File Templates.
			}
			if (request_uri.startsWith("GET /history")) {
				tempMap.put("limit", m.group(4));
			}

			if (queryString != null && queryString != "") {
				String pair[] = queryString.replace("?", "").split("&");
				for (int i = 0; i < pair.length; i++) {
					String keyValue[] = pair[i].split("=");
					tempMap.put(keyValue[0], keyValue[1]);
				}
			}
		}
		return tempMap;
	}

	/******************************** presence log parser ************************************/

	public HashMap<String, String> parse_presence(String request_uri,
			HashMap<String, String> other_tempMap) {
		HashMap<String, String> tempMap = new HashMap<String, String>();
		Matcher m = presence_matcher.matcher(request_uri);
		if (m.find()) {
			tempMap.put("sub_key", m.group(1));
			try {
				tempMap.put("channel", URLDecoder.decode(m.group(2), "utf-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace(); // To change body of catch statement use
										// File | Settings | File Templates.
			}
			if (m.group(3) != null && m.group(3).toLowerCase().equals("leave")) {
				tempMap.put("request_type", "unsubscribe");
			} else {
				tempMap.put("request_type", "presence");
			}
			String queryString = m.group(4);
			if (queryString != null) {
				String pair[] = queryString.replace("?", "").split("&");
				for (int i = 0; i < pair.length; i++) {
					String keyValue[] = pair[i].split("=");
					tempMap.put(keyValue[0], keyValue[1]);
				}
			}
		}
		return tempMap;
	}

	/***************************** updating map *********************************/

	private HashMap<String, String> updateMap(HashMap<String, String> myMap,
			HashMap<String, String> updateResult) {
		HashMap<String, String> map3 = new HashMap<String, String>();
		map3.putAll(myMap);
		map3.putAll(updateResult);
		return map3;

	}

	/************************************** input log parser *********************************/

	public HashMap<String, String> parse_line(String log_line) {

		Matcher matcher = match.matcher(log_line);
		if (matcher.find()) {
			// myMap.put("haha",log_line);

			myMap.put("client_ip", matcher.group(1));
			myMap.put("timestamp", matcher.group(2));
			String requestURI = matcher.group(3);
			myMap.put("status_code", matcher.group(4));
			myMap.put("byte_sent", matcher.group(5));
			myMap.put("referrer", matcher.group(6));
			myMap.put("user_agent", matcher.group(7));
			myMap.put("nginx_response_time", matcher.group(8));
			myMap.put("pubnub_response_time", matcher.group(9));
			myMap.put("bytes_gzip", matcher.group(19));
			myMap.put("string_length", matcher.group(21));
			myMap.put("num_messages", matcher.group(23));
			myMap.put("connections", matcher.group(24));
			myMap.put("node_id", matcher.group(27));
			myMap.put("server_ip", matcher.group(29));
			myMap.put("ssl", matcher.group(31));
			myMap.put("connection_id", matcher.group(32));
			myMap.put("session_id", "None");
			myMap.put("subscribe_timeout", "False");
			myMap.put("timetoken", matcher.group(35));
			myMap.put("origin", matcher.group(37));
			myMap.put("post_body", matcher.group(39));
			myMap.put("unix_ts", matcher.group(41));

			// Initial Housekeeping
			if (myMap.get("server_ip") == null
					|| myMap.get("server_ip").equals("-")) {
				myMap.put("server_ip", "0.0.0.0");
			}
			if (myMap.get("ssl").equals("ssl")) {
				myMap.put("ssl", "True");
			} else {
				myMap.put("ssl", "False");
			}
			if (myMap.get("connection_id") != null
					&& !myMap.get("connection_id").equals("-")) {
				myMap.put(
						"session_id",
						myMap.get("client_ip") + "-"
								+ myMap.get("connection_id"));
			}
			if (requestURI.startsWith("GET http://")) {
				Matcher mOrigion = origin_matcher.matcher(requestURI);
				if (matcher.find()) {
					myMap.put("origin", mOrigion.group(0));
					requestURI = requestURI.replace(myMap.get("origin"), "");
				}
			}
			try {

				Date date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
						.parse(myMap.get("timestamp"));
				myMap.put("timestamp",
						String.valueOf(new Long(date.getTime() / 1000)));
			} catch (ParseException e) {
				e.printStackTrace();
			}

			if (requestURI.startsWith("GET /publish")
					&& myMap.get("status_code").equals("200")) {
				myMap = updateMap(myMap, parse_publish(requestURI, myMap));

			} else if (requestURI.startsWith("GET /subscribe")
					&& myMap.get("status_code").equals("200")) {
				myMap = updateMap(myMap, parse_subscribe(requestURI, myMap));

			} else if (requestURI.startsWith("GET /subscribe")
					&& myMap.get("status_code").equals("499")) {
				myMap = updateMap(myMap, parse_unsubscribe(requestURI, myMap));

			} else if (requestURI.startsWith("GET /history")
					|| requestURI.startsWith("GET /v2/history")
					&& myMap.get("status_code").equals("200")) {
				myMap = updateMap(myMap, parse_history(requestURI, myMap));

			} else if (requestURI.startsWith("GET /v2/presence")
					&& myMap.get("status_code").equals("200")) {
				myMap = updateMap(myMap, parse_presence(requestURI, myMap));

			} else if (requestURI.startsWith("GET /time")
					&& myMap.get("status_code").equals("200")) {
				myMap.put("request_type", "time");
			} else {
				myMap.put("request_type", "unknown");
			}

		}
		return myMap;

	}

}
