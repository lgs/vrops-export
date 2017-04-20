package net.virtualviking.vropsexport;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpException;
import org.json.JSONException;
import org.json.JSONObject;

public interface DataProvider {
	public Map<String, String> fetchProps(String id) throws IOException, HttpException;
	
	public JSONObject getParentOf(String id, String parentType) throws JSONException, IOException, HttpException;
	
	public InputStream fetchMetricStream(List<JSONObject> resList, RowMetadata meta, long begin, long end) throws IOException, HttpException;
	
	public String getResourceName(String resourceId) throws JSONException, IOException, HttpException;
}
