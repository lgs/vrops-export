package net.virtualviking.vropsexport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.SSLContext;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Exporter {
	private static Log log = LogFactory.getLog(Exporter.class);

	private Config conf;

	private HttpClient client;

	private String urlBase;

	private final List<SchemaNode> schema = new ArrayList<>();

	private final Map<String, Integer> statPos = new HashMap<>();

	private Exporter parent;
		
	private Pattern parentPattern = Pattern.compile("^\\$parent\\:([_A-Za-z][_A-Za-z0-9]*)\\.(.+)$");

	private final ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 10, TimeUnit.SECONDS,
			new ArrayBlockingQueue<>(20), new ThreadPoolExecutor.CallerRunsPolicy());

	public Exporter(String urlBase, String username, String password, boolean unsafeSsl, Config conf)
			throws IOException, HttpException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException, ExporterException {
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		// Disable SSL/TLS checks if requested
		//
		if (unsafeSsl) {
			SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
				public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
					return true;
				}
			}).build();
			this.client = HttpClients.custom()
					.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE))
					.setDefaultCredentialsProvider(credentialsProvider)
					.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
		} else {
			this.client = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();
		}
		this.init(urlBase, client, conf);
	}

	private Exporter(String urlBase, HttpClient client, Config conf) throws IOException, HttpException, ExporterException {
		this.init(urlBase, client, conf);
	}

	private void init(String urlBase, HttpClient client, Config conf) throws IOException, HttpException, ExporterException {
		this.client = client;
		this.urlBase = urlBase;
		this.conf = conf;
		// Initialize the schema
		//
		this.schema.add(new SchemaNode("timestamp", "timestamp"));
		this.schema.add(new SchemaNode("resName", "resName"));
		this.registerFields();
	}
	
	public String getResourceType() {
		return conf.getResourceType();
	}

	public void exportTo(Writer out, long begin, long end, String namePattern, boolean quiet) throws IOException, HttpException {
		BufferedWriter bw = new BufferedWriter(out);

		// Output table header
		//
		boolean first = true;
		for (SchemaNode sn : this.schema) {
			if (!first) {
				bw.write(",");
			}
			first = false;
			bw.write(sn.getAlias());
		}
		bw.newLine();

		// Get all objects
		//
		String url = "/suite-api/api/resources";
		ArrayList<String> qs = new ArrayList<>();
		if(conf.getResourceType() != null) {
			qs.add("resourceKind=" + conf.getResourceType());
		}
		if(namePattern != null) {
			qs.add("name=" + namePattern);
		}
		JSONObject json = this.getJson(url, qs);
		JSONArray resources = json.getJSONArray("resourceList");
		for (int i = 0; i < resources.length(); ++i) {
			JSONObject res = resources.getJSONObject(i);
			this.executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						handleResource(bw, res, begin, end);
					} catch (Exception e) {
						log.error("Error while processing resource", e);
					}
				}
			});
			if(!quiet) {
				int pct = (100 * i) / resources.length();
				System.err.print("" + pct + "% done\r");
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(2, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			// Shouldn't happen...
			//
			e.printStackTrace();
			return;
		}
		bw.flush();
		if(!quiet)
			System.err.println("100% done");
	}
	
	private JSONArray fetchJsonMetrics(JSONObject res, long begin, long end) throws IOException, HttpException {
		String resId = res.getString("identifier");

		// Load metric history for all metrics
		//
		String url = "/suite-api/api/resources/stats";
		ArrayList<String> qs = new ArrayList<>();
		qs.add("resourceId=" + resId);
		qs.add("intervalType=MINUTES");
		qs.add("intervalQuantifier=" + conf.getRollupMinutes());
		qs.add("rollUpType=AVG");
		qs.add("begin=" + begin);
		qs.add("end=" + end);
		for (Config.Field fld : conf.getFields()) {
			if(fld.getName().startsWith("$parent"))
				continue;
			qs.add("statKey=" + fld.getName().replaceAll("\\|", "%7C"));
		}
		JSONObject metricJson = this.getJson(url, qs);
		JSONArray mr = metricJson.getJSONArray("values");
		JSONObject statsNode = mr.getJSONObject(0);
		return statsNode.getJSONObject("stat-list").getJSONArray("stat");
	}

	private TreeMap<Long, List<Object>> fetchMetrics(JSONObject res, long begin, long end) throws IOException, HttpException {
		TreeMap<Long, List<Object>> rows = new TreeMap<Long, List<Object>>();
		String resId = res.getString("identifier");
		String resName = res.getJSONObject("resourceKey").getString("name");
		JSONArray myContent = this.fetchJsonMetrics(res, begin, end);
		List<JSONArray> contentList = new ArrayList<>(2);
		contentList.add(myContent);
		
		// Are we fetching metrics from the parent?
		//
		if(parent != null) {
			// Fetch metrics
			//
			JSONObject p = this.getParentOf(resId, parent.getResourceType());
			JSONArray parentContent = parent.fetchJsonMetrics(res, begin, end);
			
			// Splice with rest of metrics
			//
			// Put metric names back on the $parent:ResourceKind.metric form
			//
			for (int j = 0; j < parentContent.length(); ++j) {
				JSONObject contentNode = parentContent.getJSONObject(j);
				JSONObject statKey = contentNode.getJSONObject("statKey");
				statKey.put("key", "$parent:" + parent.getResourceType() + "." + statKey.getString("key"));
			}
			contentList.add(parentContent);
		}

		// It's really just the fetching we're interested in running in
		// parallel. The in-memory processing should
		// be fast, so in order to keep the code simple, we hold a lock during
		// the entire processing and writing phase.
		//
		synchronized (this) {
			for(JSONArray chunk : contentList) {

				// Handle each stat key
				//
				for (int j = 0; j < chunk.length(); ++j) {
					JSONObject contentNode = chunk.getJSONObject(j);
					String statKey = contentNode.getJSONObject("statKey").getString("key");
					if (!this.statPos.containsKey(statKey))
						continue;
					int statIdx = this.statPos.get(statKey);
	
					// Handle timestamps
					//
					JSONArray jsonTs = contentNode.getJSONArray("timestamps");
					List<Long> timestamps = new ArrayList<Long>();
					for (int k = 0; k < jsonTs.length(); ++k) {
						long t = jsonTs.getLong(k);
						timestamps.add(t);
					}
	
					// Handle values
					//
					JSONArray jsonValues = contentNode.getJSONArray("data");
					for (int k = 0; k < jsonValues.length(); ++k) {
						long t = timestamps.get(k);
	
						// Fill row with nulls up until where we're about to insert
						// the data.
						//
						List<Object> row = rows.get(t);
						if (row == null) {
							row = new ArrayList<Object>();
							row.add(t);
							row.add(resName);
							rows.put(t, row);
						}
						while (row.size() <= statIdx) {
							row.add(null);
						}
						row.set(statIdx, jsonValues.get(k));
					}
				}
			}
		}
		return rows;
	}
	
	private JSONObject getParentOf(String id, String parentType) throws JSONException, IOException, HttpException {
		JSONObject json = this.getJson("/suite-api/api/resources/" + id + "/relationships", "relationshipType=PARENT");
		JSONArray rl = json.getJSONArray("resourceList");
		for(int i = 0; i < rl.length(); ++i) {
			JSONObject r = rl.getJSONObject(i);
			
			// If there's more than one we only return the first one.
			//
			if(r.getJSONObject("resourceKey").getString("resourceKindKey").equals(parentType))
				return r;
		}
		return null;
	}

	private void handleResource(BufferedWriter bw, JSONObject res, long begin, long end) throws IOException, HttpException {
		TreeMap<Long, List<Object>> rows = this.fetchMetrics(res, begin, end);
		synchronized (this) {
			// Done collecting data, now write it!
			//
			for (Map.Entry<Long, List<Object>> entry : rows.entrySet()) {
				boolean first = true;
				for (Object o : entry.getValue()) {
					if (!first) {
						bw.write(",");
					}
					first = false;
					bw.write(o != null ? o.toString() : "");
				}
				bw.newLine();
			}
		}
	}

	private void registerFields() throws IOException, HttpException, ExporterException {
		String parentType = null;
    	List<Config.Field> parentFields = new ArrayList<Config.Field>();
        for(Config.Field fld : conf.getFields()) {
        	String name = fld.getName();
        	
        	// Parse parent reference if present.
        	//
        	Matcher m = parentPattern.matcher(name);
        	if(m.matches()) {
        		String pn = m.group(1);
        		if(parentType == null) {
        			parentType = pn;
        		} else if(!pn.equals(parentType)) 
        			throw new ExporterException("References to multiple parents not supported");
        		String fn = m.group(2);
        		parentFields.add(new Config.Field(fld.getAlias(), fn));
        	}
            if(statPos.get(fld.getName()) == null) {
                schema.add(new SchemaNode(name, fld.getAlias()));
                statPos.put(name, schema.size() - 1);
            }
        }
        
        // Handle parent fields
        //
        if(parentFields.size() > 0) {
        	Config parentConfig = new Config();
        	Config.Field[] pf = new Config.Field[parentFields.size()];
        	parentFields.toArray(pf);
        	parentConfig.setFields(pf);
        	parentConfig.setResourceType(parentType);
        	parentConfig.setRollupMinutes(this.conf.getRollupMinutes());
        	parentConfig.setRollupType(this.conf.getRollupType());
        	this.parent = new Exporter(urlBase, client, parentConfig);
        }
    }

	private JSONObject getJson(String uri, String ...queries) throws IOException, HttpException {
		if(queries != null) {
			for(int i = 0; i < queries.length; ++i) {
				uri += i == 0 ? '?' : '&';
				uri += queries[i];
			}
		}
		HttpGet get = new HttpGet(urlBase + uri);
		get.addHeader("accept", "application/json");
		HttpResponse resp = client.execute(get);
		this.checkResponse(resp);
		return new JSONObject(EntityUtils.toString(resp.getEntity()));
	}
	
	private JSONObject getJson(String uri, List<String> queries) throws IOException, HttpException {
		String[] s;
		if(queries != null) {
			s = new String[queries.size()];
			queries.toArray(s);
		} else
			s = new String[0];
		return this.getJson(uri, s);
	}

	private HttpResponse checkResponse(HttpResponse response)
			throws HttpException, UnsupportedOperationException, IOException {
		int status = response.getStatusLine().getStatusCode();
		log.debug("HTTP status: " + status);
		if (status == 200 || status == 201)
			return response;
		log.debug("Error response from server: "
				+ IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset()));
		throw new HttpException("HTTP Error: " + response.getStatusLine().getStatusCode() + " Reason: "
				+ response.getStatusLine().getReasonPhrase());
	}
}
