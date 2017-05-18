package net.virtualviking.vropsexport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
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
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

public class Client {
	private static Log log = LogFactory.getLog(Client.class);
	
	private static final int CONNTECTION_TIMEOUT_MS = 60000;
	
	private static final int CONNECTION_REQUEST_TIMEOUT_MS = 60000;
	
	private static final int SOCKET_TIMEOUT_MS = 60000;
	
	private final HttpClient client;
	
	private final String urlBase;
	
	private final  LRUCache<String, JSONObject> jsonCache = new LRUCache<>(1000);
	
	private String authToken;
	
	public Client(String urlBase, String username, String password, boolean unsafeSsl, int threads) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, IOException, HttpException {
		this.urlBase = urlBase;
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

	public JSONObject getJson(String uri, String ...queries) throws IOException, HttpException {
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
	
	public InputStream postJsonReturnStream(String uri, JSONObject payload) throws IOException, HttpException {
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
	
	public JSONObject getJson(String uri, List<String> queries) throws IOException, HttpException {
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