package conf;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import ninja.utils.NinjaProperties;

@Singleton
public class StartupActions {

	private NinjaProperties ninjaProperties;

	@Inject
	public StartupActions(NinjaProperties ninjaProperties) {
		this.ninjaProperties = ninjaProperties;
	}
}
