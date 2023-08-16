package org.apache.cassandra.stress.settings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;

/**
 * For specifying storage options
 */
class OptionStorage extends OptionMulti
{
    private final OptionSimple type = new OptionSimple("type=", "LOCAL|S3", null, "The storage backend to use", false);
    private final OptionSimple endpoint = new OptionSimple("endpoint=", ".*", null, "The server address of S3 server", false);
    private final OptionSimple bucket = new OptionSimple("bucket=", ".*", null, "The bucket hosting the keyspace", false);

    public OptionStorage()
    {
        super("storage", "Define the storage backend parameters", true);
    }

    public String getType()
    {
        return type.value();
    }

    public Map<String, String> getOptions()
    {
        Map<String, String> options = extraOptions();
        if (endpoint.present()) {
            options.put("endpoint", endpoint.value());
        }
        if (bucket.present()) {
            options.put("bucket", bucket.value());
        }
        return options;
    }

    protected List<? extends Option> options()
    {
        return Arrays.asList(type, endpoint, bucket);
    }

    @Override
    public boolean happy()
    {
        if (!type.present()) {
            return true;
        }
        if (type.value().equals("LOCAL")) {
            return true;
        }
        return endpoint.present() && bucket.present();
    }
}
