package net.virtualviking.vropsexport;

import java.io.Reader;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class ConfigLoader {
	static public Config parse(Reader rdr) {
		Yaml yaml = new Yaml(new Constructor(Config.class));
		return (Config) yaml.load(rdr);
	}
}
