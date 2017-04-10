package net.virtualviking.vropsexport;

public class Config {
	public static class Field {
		private String alias;
		private String name;
		
		public Field() {
		}
	
		public Field(String alias, String name) {
			super();
			this.alias = alias;
			this.name = name;
		}

		public String getAlias() {
			return alias;
		}

		public void setAlias(String alias) {
			this.alias = alias;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
	private Field[] fields;
	private String resourceType;
	private String rollupType;
	private long rollupMinutes;
	
	public Config() {
	}

	public Field[] getFields() {
		return fields;
	}

	public void setFields(Field[] fields) {
		this.fields = fields;
	}

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public String getRollupType() {
		return rollupType;
	}

	public void setRollupType(String rollupType) {
		this.rollupType = rollupType;
	}

	public long getRollupMinutes() {
		return rollupMinutes;
	}

	public void setRollupMinutes(long rollup) {
		this.rollupMinutes = rollup;
	}
}
