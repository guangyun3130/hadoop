package org.apache.hadoop.fs.qinu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

import java.io.IOException;
import java.net.URI;

public class QiniuKodoContract extends AbstractBondedFSContract {
    private static final String CONTRACT_XML = "qiniu-kodo/contract.xml";

    /**
     * Constructor: loads the authentication keys if found
     *
     * @param conf configuration to work with
     */
    public QiniuKodoContract(Configuration conf) {
        super(conf);
        addConfResource(CONTRACT_XML);
    }

    @Override
    public FileSystem getFileSystem(URI uri) throws IOException {
        FileSystem fs = super.getFileSystem(uri);
        fs.delete(getTestPath(), true);
        return fs;
    }

    @Override
    public FileSystem getTestFileSystem() throws IOException {
        FileSystem fs = super.getTestFileSystem();
        fs.delete(getTestPath(), true);
        return fs;
    }

    @Override
    public String getScheme() {
        return "kodo";
    }

    public synchronized static Configuration getConfiguration() {
        Configuration cfg = new Configuration();
        cfg.addResource(CONTRACT_XML);
        return cfg;
    }
}