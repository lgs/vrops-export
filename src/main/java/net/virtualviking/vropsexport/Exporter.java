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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

	private final Map<String, Integer> statPos = new HashMap<>();

	private Exporter parent;
		
	private Pattern parentPattern = Pattern.compile("^\\$parent\\:([_A-Za-z][_A-Za-z0-9]*)\\.(.+)$");
	
	private Pattern parentSpecPattern = Pattern.compile("^([_\\-A-Za-z][_\\-A-Za-z0-9]*):(.+)$");
	
	private LRUCache<String, JSONObject> jsonCache = new LRUCache<>(1000);
	
	private DateFormat dateFormat;
	
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
		if(this.conf.getDateFormat() != null)
			this.dateFormat = new SimpleDateFormat(this.conf.getDateFormat());
		this.registerFields();
	}
	
	public String getResourceType() {
		return conf.getResourceType();
	}
	
	public boolean hasProps() {
		return conf.hasProps();
	}

	public void exportTo(Writer out, long begin, long end, String namePattern, String parentSpec, boolean quiet) throws IOException, HttpException, ExporterException {
		BufferedWriter bw = new BufferedWriter(out);

		// Output table header
		//
		bw.write("timestamp,resName");
		for (Config.Field fld : this.conf.getFields()) {
			bw.write(",");
			bw.write(fld.getAlias());
		}
		bw.newLine();

		JSONArray resources;
		if(parentSpec != null) {
			// Lookup parent
			//
			Matcher m = parentSpecPattern.matcher(parentSpec);
			if(!m.matches())
				throw new ExporterException("Not a valid parent spec: " + parentSpec + ". should be on the form ResourceKind:resourceName");
			JSONArray pResources = this.fetchResources(m.group(1), m.group(2));
			if(pResources.length() == 0) 
				throw new ExporterException("Parent not found");
			if(pResources.length() > 1)
				throw new ExporterException("Parent spec is not unique");
			String pId = pResources.getJSONObject(0).getString("identifier");
			
			// Get children
			//
			String url = "/suite-api/api/resources/" + pId + "/relationships";
			resources = this.getJson(url, "relationshipType=CHILD").getJSONArray("resourceList");
		} else {
			// Get all objects, possibly filtered by name
			//
			resources = this.fetchResources(conf.getResourceType(), namePattern);
		} 
		for (int i = 0; i < resources.length(); ++i) {
			JSONObject res = resources.getJSONObject(i);
			
			// Child relationships may return objects of the wrong type, so we have
			// to check the type here.
			//
			if(!res.getJSONObject("resourceKey").getString("resourceKindKey").equals(conf.getResourceType()))
				continue;
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
	
	private JSONArray fetchResources(String resourceKind, String name) throws JSONException, IOException, HttpException {
		String url = "/suite-api/api/resources";
		ArrayList<String> qs = new ArrayList<>();
		qs.add("resourceKind=" + resourceKind);
		if(name != null)
			qs.add("name=" + name);
		return this.getJson(url, qs).getJSONArray("resourceList");
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
			if(!fld.hasMetric()) 
				continue;
			if(fld.getMetric().startsWith("$parent"))
				continue;
			qs.add("statKey=" + fld.getMetric().replaceAll("\\|", "%7C"));
		}
		JSONObject metricJson = this.getJson(url, qs);
		JSONArray mr = metricJson.getJSONArray("values");
		
		// No values? Return an empty array!
		//
		if(mr.length() == 0)
			return new JSONArray();
		JSONObject statsNode = mr.getJSONObject(0);
		return statsNode.getJSONObject("stat-list").getJSONArray("stat");
	}
	
	private JSONArray fetchJsonProps(JSONObject res) throws IOException, HttpException {
		String id = res.getString("identifier");
		String uri = "/suite-api/api/resources/" + id + "/properties";
		JSONObject json = this.getJson(uri);
		JSONArray result = json.getJSONArray("property");
		return result;
	}

	private TreeMap<Long, List<Object>> fetchMetrics(JSONObject res, long begin, long end) throws IOException, HttpException {
		TreeMap<Long, List<Object>> rows = new TreeMap<Long, List<Object>>();
		String resId = res.getString("identifier");
		String resName = res.getJSONObject("resourceKey").getString("name");
		JSONArray myContent = this.fetchJsonMetrics(res, begin, end);
		List<JSONArray> contentList = new ArrayList<>(2);
		contentList.add(myContent);
		List<JSONArray> propList = new ArrayList<>(2);
		if(conf.hasProps())
			propList.add(this.fetchJsonProps(res));
		
		// Are we fetching metrics from the parent?
		//
		if(parent != null) {
			// Fetch metrics
			//
			JSONObject p = this.getParentOf(resId, parent.getResourceType());
			
			// Proceed only if parent was found
			//
			if(p != null) {
				JSONArray parentContent = parent.fetchJsonMetrics(p, begin, end);
				
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
				
				// Handle parent properties
				//
				if(parent.hasProps()) {
					JSONArray parentProps = this.fetchJsonProps(p);
					
					// Put property names back on the $parent:ResourceKind.metric form
					//
					for (int j = 0; j < parentProps.length(); ++j) {
						JSONObject pp = parentProps.getJSONObject(j);
						pp.put("name", "$parent:" + parent.getResourceType() + "." + pp.getString("name"));
					}
					propList.add(parentProps);
				}
			}
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
			
			// Handle properties
			// TODO: Could be optimized by filling a sparse vector with the properties
			// and copying it to each row.
			//
			for(JSONArray props : propList) {
				for(List<Object> row : rows.values()) {
					for(int i = 0; i < props.length(); ++i) {
						JSONObject propBundle = props.getJSONObject(i);
						String name = propBundle.getString("name");
						if(!statPos.containsKey(name))
							continue;
						int idx = statPos.get(name);
						while (row.size() <= idx) {
							row.add(null);
						}
						row.set(idx, propBundle.get("value"));
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
				Iterator<Object> itor = entry.getValue().iterator();
				
				// Deal with timestamp
				//
				long t = (long) itor.next();
				if(dateFormat != null) {
					bw.write("\"" + dateFormat.format(new Date(t)) + "\"");
				} else
					bw.write("\"" + t + "\"");
				while(itor.hasNext()) {
					Object o = itor.next();
					bw.write(",\"");
					bw.write(o != null ? o.toString() : "");
					bw.write('"');
				}
				bw.newLine();
			}
			bw.flush();
		}
	}

	private void registerFields() throws IOException, HttpException, ExporterException {
		String parentType = null;
    	List<Config.Field> parentFields = new ArrayList<Config.Field>();
    	int pos = 2; // Advance past timestamp and resource name
        for(Config.Field fld : conf.getFields()) {
        	boolean isMetric = fld.hasMetric();
        	String name = isMetric ? fld.getMetric() : fld.getProp();
        	
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
        		parentFields.add(new Config.Field(fld.getAlias(), fn, isMetric));
        	}
            if(statPos.get(fld.getMetric()) == null) {
                statPos.put(name, pos);
            }
            ++pos;
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
		synchronized(jsonCache) {
			if(jsonCache.containsKey(uri)) {
				return jsonCache.get(uri);
			}
		}
		HttpGet get = new HttpGet(urlBase + uri);
		get.addHeader("accept", "application/json");
		HttpResponse resp = client.execute(get);
		this.checkResponse(resp);
		JSONObject result = new JSONObject(EntityUtils.toString(resp.getEntity()));
		synchronized(jsonCache) {
			jsonCache.put(uri, result);
		}
		return result;
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
