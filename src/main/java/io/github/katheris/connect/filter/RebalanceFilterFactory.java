package io.github.katheris.connect.filter;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = Void.class)
public class RebalanceFilterFactory implements FilterFactory<Void, Void> {

    @Override
    public Void initialize(FilterFactoryContext context, Void config) throws PluginConfigurationException {
        return null;
    }

    @Override
    public RebalanceFilter createFilter(FilterFactoryContext context, Void configuration) {
        return new RebalanceFilter();
    }
}
