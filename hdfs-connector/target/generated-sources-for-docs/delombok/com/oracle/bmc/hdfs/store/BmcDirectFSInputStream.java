/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;
import com.oracle.bmc.hdfs.util.FSStreamUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import com.google.common.base.Supplier;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;

/**
 * Direct stream implementation for reading files. Seeking is achieved by closing the stream and recreating using a
 * range request to the new position.
 */
public class BmcDirectFSInputStream extends BmcFSInputStream {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BmcDirectFSInputStream.class);

    public BmcDirectFSInputStream(final ObjectStorage objectStorage, final FileStatus status, final Supplier<GetObjectRequest.Builder> requestBuilder, final Statistics statistics) {
        super(objectStorage, status, requestBuilder, statistics);
    }

    @Override
    protected long doSeek(final long position) throws IOException {
        // close the current stream and let the validation step recreate it at the requested position
        IOUtils.closeQuietly(super.getSourceInputStream());
        super.setSourceInputStream(null);
        super.validateState(position);
        return super.getPos();
    }

    @Override
    public int read() throws IOException {
        // Try reading from the current stream
        try {
            return super.read();
        } catch (IOException e) {
            LOG.warn("Read failed, possibly a stale connection. Will re-attempt.", e);
            // If the stream has been idle for a while, then Object Storage LB closes the connection causing an IOException
            // on the client side. Close the current stream and try again.
            FSStreamUtils.closeQuietly(super.getSourceInputStream());
            super.setSourceInputStream(null);
            return super.read();
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        // Try reading from the current stream
        try {
            return super.read(b, off, len);
        } catch (IOException e) {
            LOG.warn("Read failed, possibly a stale connection. Will re-attempt.", e);
            // If the stream has been idle for a while, then Object Storage LB closes the connection causing an IOException
            // on the client side. Close the current stream and try again.
            FSStreamUtils.closeQuietly(super.getSourceInputStream());
            super.setSourceInputStream(null);
            return super.read(b, off, len);
        }
    }
}
