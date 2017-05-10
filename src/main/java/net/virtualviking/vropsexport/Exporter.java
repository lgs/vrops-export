/* 
 * Copyright 2017 Pontus Rydin
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package net.virtualviking.vropsexport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import net.virtualviking.vropsexport.processors.CSVPrinter;
import net.virtualviking.vropsexport.processors.SQLDumper;

import javax.net.ssl.SSLContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
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

public class Exporter implements DataProvider {
	private class Progress implements ProgressMonitor {
		private final int totalRows;
		
		private int rowsProcessed = 0;
		
		public Progress(int totalRows) {
			this.totalRows = totalRows;
		}
		
		public synchronized void reportProgress(int n) {
			this.rowsProcessed += n;
			int pct = (100 * rowsProcessed) / totalRows;
			System.err.print("" + pct + "% done\r");
		}
	}
	private static Log log = LogFactory.getLog(Exporter.class);

	private Config conf;

	private HttpClient client;

	private String urlBase;

	private final Map<String, Integer> statPos = new HashMap<>();
	
	private final LRUCache<String, String> nameCache = new LRUCache<>(1000);
	
	private LRUCache<String, JSONObject> jsonCache = new LRUCache<>(1000);
	
	private LRUCache<String, Rowset> rowsetCache = new LRUCache<>(200); 
	
	private DateFormat dateFormat;
	
	private static final int MAX_RESPONSE_ROWS = 100000; // TODO: This is a wild guess. It seems vR Ops barfs on responses that are too long.
	
	private static final int MAX_CHUNKSIZE = 10;
	
	private static final int CONNTECTION_TIMEOUT_MS = 60000;
	
	private static final int CONNECTION_REQUEST_TIMEOUT_MS = 60000;
	
	private static final int SOCKET_TIMEOUT_MS = 60000;
	
	private static final int PAGE_SIZE = 1000;
	
	private String authToken;
	
	private final boolean verbose;
	
	private final boolean useTempFile;
	
	private ThreadPoolExecutor executor;
	
	private RowsetProcessorFacotry rspFactory;
	
	private static final Map<String, RowsetProcessorFacotry> rspFactories = new HashMap<>();
	
	static {
		rspFactories.put("sql", new SQLDumper.Factory());
		rspFactories.put("csv", new CSVPrinter.Factory());
	}

	public Exporter(String urlBase, String username, String password, boolean unsafeSsl, int threads, Config conf, boolean verbose, boolean useTempFile)
			throws IOException, HttpException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException, ExporterException {
		this.rspFactory = rspFactories.get(conf.getOutputFormat());
		if(rspFactory == null)
			throw new ExporterException("Unknown output format: " + conf.getOutputFormat());
		
		// Configure timeout
		//
		final RequestConfig requestConfig = RequestConfig.custom()
			    .setConnectTimeout(CONNTECTION_TIMEOUT_MS)
			    .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MS)
			    .setSocketTimeout(SOCKET_TIMEOUT_MS)
			    .build();
		
		// Disable SSL/TLS checks if requested
		//
		if (unsafeSsl) {
			SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
				public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
					return true;
				}
			}).build();
			SSLConnectionSocketFactory sslf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create().register("https", sslf).build();
			PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
			cm.setMaxTotal(20);
			cm.setDefaultMaxPerRoute(20);
			this.client = HttpClients.custom().
					setSSLSocketFactory(sslf).
					setConnectionManager(cm).
				    setDefaultRequestConfig(requestConfig).
					//setRetryHandler(new DefaultHttpRequestRetryHandler(3, false)).
					setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
		} else {
			PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
			cm.setMaxTotal(threads * 4);
			cm.setDefaultMaxPerRoute(threads * 4);
			this.client = HttpClientBuilder.create().
					setConnectionManager(cm).
					setDefaultRequestConfig(requestConfig).
					//setRetryHandler(new DefaultHttpRequestRetryHandler(3, false)).
					build();
		}
		this.verbose = verbose;
		this.useTempFile = useTempFile;
		
		// Do basic initialization
		//
		this.init(urlBase, client, conf, threads);

		// Authenticate
		//
		JSONObject rq = new JSONObject();
		rq.put("username", username);
		rq.put("password", password);
		InputStream is = this.postJsonReturnStream("/suite-api/api/auth/token/acquire", rq);
		try {
			JSONObject response = new JSONObject(IOUtils.toString(is, Charset.defaultCharset()));
			this.authToken = response.getString("token");
		} finally {
			is.close();
		}
	}

	private void init(String urlBase, HttpClient client, Config conf, int threads) throws IOException, HttpException, ExporterException {
		this.client = client;
		this.urlBase = urlBase;
		this.conf = conf;
		
		// Calling this with a null conf is only valid if we're printing field names and nothing else. 
		// Everything else will crash miserably! (Yeah, this is a bit of a hack...)
		//
		if(conf != null) {
			if(this.conf.getDateFormat() != null)
				this.dateFormat = new SimpleDateFormat(this.conf.getDateFormat());
			this.registerFields();
		}
		this.executor = new ThreadPoolExecutor(threads, threads, 5, TimeUnit.SECONDS,
					new ArrayBlockingQueue<>(20), new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	public boolean hasProps() {
		return conf.hasProps();
	}

	public void exportTo(Writer out, long begin, long end, String namePattern, String parentSpec, boolean quiet) throws IOException, HttpException, ExporterException {
		BufferedWriter bw = new BufferedWriter(out);
		Progress progress = null;
		
		// Create RowsetProcessor
		//
		RowMetadata meta = new RowMetadata(conf);
		RowsetProcessor rsp = rspFactory.makeFromConfig(bw, conf, this, progress);
		rsp.preample(meta, conf);
		JSONArray resources;
		String parentId = null;
		if(parentSpec != null) {
			// Lookup parent
			//
			Matcher m = Patterns.parentSpecPattern.matcher(parentSpec);
			if(!m.matches())
				throw new ExporterException("Not a valid parent spec: " + parentSpec + ". should be on the form ResourceKind:resourceName");
			// TODO: No way of specifying adapter type here. Should there be?
			//
			JSONArray pResources = this.fetchResources(m.group(1), null, m.group(2), 0).getJSONArray("resourceList");
			if(pResources.length() == 0) 
				throw new ExporterException("Parent not found");
			if(pResources.length() > 1)
				throw new ExporterException("Parent spec is not unique");
			parentId = pResources.getJSONObject(0).getString("identifier");
		} 
	
		int page = 0;
		for(;;) {
			JSONObject resObj = null;
			
			// Fetch resources
			//
			if(parentId != null) {
				String url = "/suite-api/api/resources/" + parentId + "/relationships";
				resObj = this.getJson(url, "relationshipType=CHILD", "page=" + page++);
			} else
				resObj = this.fetchResources(conf.getResourceKind(), conf.getAdapterKind(), namePattern, page++);
			resources = resObj.getJSONArray("resourceList");
			
			// If we got an empty set back, we ran out of pages.
			//
			if(resources.length() == 0)
				break;
			
			// Initialize progress reporting
			//
			if(!quiet && progress == null) {
				progress = new Progress(resObj.getJSONObject("pageInfo").getInt("totalCount"));
				progress.reportProgress(0);
			}
			
			// Canculate a suitable chunk size by assuming that responses should be kept shorter than MAX_RESPONSE_ROWS.
			//
			long estimatedRows = conf.getFields().length * (end - begin) / (conf.getRollupMinutes() * 60000);
			int chunkSize = (int) Math.min(Math.max(MAX_RESPONSE_ROWS / estimatedRows, 1), MAX_CHUNKSIZE);
			ArrayList<JSONObject> chunk = new ArrayList<>(chunkSize);
			for (int i = 0; i < resources.length(); ++i) {
				JSONObject res = resources.getJSONObject(i);
				synchronized(nameCache) {
					nameCache.put(res.getString("identifier"), res.getJSONObject("resourceKey").getString("name"));
				}
				chunk.add(res);
				if(chunk.size() >= chunkSize || i == resources.length() - 1) { 
					
					// Child relationships may return objects of the wrong type, so we have
					// to check the type here.
					//
					JSONObject rKey = res.getJSONObject("resourceKey");
					if(!rKey.getString("resourceKindKey").equals(conf.getResourceKind()))
						continue;
					if(conf.getAdapterKind() != null && !rKey.getString("adapterKindKey").equals(conf.getAdapterKind()))
						continue;
					this.startChunkJob(bw, chunk, rsp, meta, begin, end, progress);
					chunk = new ArrayList<>(chunkSize);
				}
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
	
	private void startChunkJob(BufferedWriter bw, List<JSONObject> chunk, RowsetProcessor rsp, RowMetadata meta, long begin, long end, Progress progress) {
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					handleResources(bw, chunk, rsp, meta, begin, end, progress);
				} catch (Exception e) {
					log.error("Error while processing resource", e);
				}
			}
		});
	}
	
	private JSONObject fetchResources(String resourceKind, String adapterKind, String name, int page) throws JSONException, IOException, HttpException {
		String url = "/suite-api/api/resources";
		ArrayList<String> qs = new ArrayList<>();
		if(adapterKind != null)
			qs.add("adapterKind=" + adapterKind);
		qs.add("resourceKind=" + resourceKind);
		qs.add("pageSize=" + PAGE_SIZE);
		qs.add("page=" + page);
		if(name != null)
			qs.add("name=" + name);
		JSONObject response = this.getJson(url, qs); 
		if(verbose)
			System.err.println("Resources found: " + response.getJSONObject("pageInfo").getInt("totalCount"));
		return response;
	}
	
	public String getResourceName(String resourceId) throws JSONException, IOException, HttpException {
		synchronized(nameCache) {
			String name = nameCache.get(resourceId);
			if(name != null)
				return name;
		}
		String url = "/suite-api/api/resources/" + resourceId;
		JSONArray resList = this.getJson(url).getJSONArray("resourceList");
		if(resList.length() == 0)
			return null;
		String name = resList.getJSONObject(0).getJSONObject("resourceKey").getString("name");
		synchronized(nameCache) {
			nameCache.put(resourceId, name);
		}
		return name;
	}
	
	public InputStream fetchMetricStream(List<JSONObject> resList, RowMetadata meta, long begin, long end) throws IOException, HttpException {
		JSONObject q = new JSONObject();
		JSONArray ids = new JSONArray();
		for(JSONObject res : resList) 
			ids.put(res.getString("identifier"));
		q.put("resourceId", ids);
		q.put("intervalType", "MINUTES");
		q.put("intervalQuantifier", conf.getRollupMinutes());
		q.put("rollUpType", "AVG");
		q.put("begin", begin);
		q.put("end", end);
		JSONArray stats = new JSONArray();
		for(String f : meta.getMetricMap().keySet())
			stats.put(f);
		q.put("statKey", stats);
		return this.postJsonReturnStream("/suite-api/api/resources/stats/query", q);
	}
	
	public Map<String, String> fetchProps(String id) throws IOException, HttpException {
		String uri = "/suite-api/api/resources/" + id + "/properties";
		JSONObject json = this.getJson(uri);
		JSONArray props = json.getJSONArray("property");
		Map<String, String> result = new HashMap<>(props.length());
		for(int i = 0; i < props.length(); ++i) {
			JSONObject p = props.getJSONObject(i);
			result.put(p.getString("name"), p.getString("value"));
		}
		return result;
	}

	public JSONObject getParentOf(String id, String parentType) throws JSONException, IOException, HttpException {
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
	
	public void printResourceMetadata(String adapterAndResourceKind, PrintStream out) throws IOException, HttpException {
		String resourceKind = adapterAndResourceKind;
		String adapterKind = "VMWARE";
		Matcher m = Patterns.adapterAndResourceKindPattern.matcher(adapterAndResourceKind);
		if(m.matches()) {
			adapterKind = m.group(1);
			resourceKind = m.group(2);
		}
		JSONObject response = this.getJson("/suite-api/api/adapterkinds/" + adapterKind + "/resourcekinds/" + resourceKind + "/statkeys");
		JSONArray stats = response.getJSONArray("resourceTypeAttributes");
		for(int i = 0; i < stats.length(); ++i) {
			JSONObject stat = stats.getJSONObject(i);
			out.println("Key  : " + stat.getString("key"));
			out.println("Name : " + stat.getString("name"));
			out.println();
		}
	}
	
	public void printResourceKinds(String adapterKind, PrintStream out) throws IOException, HttpException {
		if(adapterKind == null) 
			adapterKind = "VMWARE";
		JSONObject response = this.getJson("/suite-api/api/adapterkinds/" + adapterKind + "/resourcekinds");
		JSONArray kinds = response.getJSONArray("resource-kind");
		for(int i = 0; i < kinds.length(); ++i) {
			JSONObject kind = kinds.getJSONObject(i);
			out.println("Key  : " + kind.getString("key"));
			out.println("Name : " + kind.getString("name"));
			out.println();
		}
	}

	private void handleResources(BufferedWriter bw, List<JSONObject> resList, RowsetProcessor rsp, RowMetadata meta, long begin, long end, ProgressMonitor progress) throws IOException, HttpException, ExporterException {
		InputStream content = null;
		try {
			long start = System.currentTimeMillis();
			content = this.fetchMetricStream(resList, meta, begin, end);
			if(verbose)
				System.err.println("Metric request call took " + (System.currentTimeMillis() - start) + " ms");
		} catch(NoHttpResponseException e) {
			// This seems to happen when we're giving the server too much work to do in one call.
			// Try again, but split the chunk into two and run them separately.
			//
			int sz = resList.size();
			if(sz <= 1) {
				// Already down to one item? We're out of luck!
				//
				throw new ExporterException(e);
			}
			// Split lists and try them separately
			//
			int half = sz / 2;
			log.warn("Server closed connection. Trying smaller chunk (current=" + sz + ")");
			List<JSONObject> left = new ArrayList<>(half);
			List<JSONObject> right = new ArrayList<>(sz - half);
			int i = 0;
			while(i < half) 
				left.add(resList.get(i++));
			while(i < sz)
				right.add(resList.get(i++));
			this.handleResources(bw, left, rsp, meta, begin, end, progress);
			this.handleResources(bw, right, rsp, meta, begin, end, progress);
			return;
		}
		try {
			if(useTempFile) {
				// Dump to temp file
				//
				File tmpFile = null;
				long start = System.currentTimeMillis();
				try {
					tmpFile = File.createTempFile("vrops-export", ".tmp");
					FileOutputStream out = new FileOutputStream(tmpFile);
					try {
						IOUtils.copy(content, out);
					} finally {
						out.close();
					}
					
				} finally {
					content.close();
				}
				content = new SelfDeletingFileInputStream(tmpFile);
				if(verbose)
					System.err.println("Dumping to temp file took " + (System.currentTimeMillis() - start) + " ms");
			}
			long start = System.currentTimeMillis();
			StatsProcessor sp = new StatsProcessor(conf, this, rowsetCache, verbose);
			sp.process(content,  rspFactory.makeFromConfig(bw, conf, this, progress), begin, end);
			if(verbose)
				System.err.println("Result processing took " + (System.currentTimeMillis() - start) + " ms");
		} finally {
			content.close();
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
        	Matcher m = Patterns.parentPattern.matcher(name);
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
		get.addHeader("Accept", "application/json");
		if(this.authToken != null)
			get.addHeader("Authorization", "vRealizeOpsToken " + this.authToken + "");
		HttpResponse resp = client.execute(get);
		this.checkResponse(resp);
		JSONObject result = new JSONObject(EntityUtils.toString(resp.getEntity()));
		synchronized(jsonCache) {
			jsonCache.put(uri, result);
		}
		return result;
	}
	
	private InputStream postJsonReturnStream(String uri, JSONObject payload) throws IOException, HttpException {
		HttpPost post = new HttpPost(urlBase + uri);
		post.setEntity(new StringEntity(payload.toString()));
		//System.err.println(payload.toString());
		post.addHeader("Accept", "application/json");
		post.addHeader("Content-Type", "application/json");
		if(this.authToken != null)
			post.addHeader("Authorization", "vRealizeOpsToken " + this.authToken + "");
		HttpResponse resp = client.execute(post);
		this.checkResponse(resp);
		return resp.getEntity().getContent();
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
