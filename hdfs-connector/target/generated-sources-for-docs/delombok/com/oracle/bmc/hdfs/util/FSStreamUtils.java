/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.util;

import org.apache.commons.io.IOUtils;
import javax.ws.rs.ProcessingException;
import java.io.InputStream;

public class FSStreamUtils {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(FSStreamUtils.class);

    /**
     * Closes an <code>InputStream</code> unconditionally.
     * <p>
     * Equivalent to {@link InputStream#close()}, except any exceptions will be ignored.
     *
     * @param stream the InputStream to close, may be null or already closed
     */
    public static void closeQuietly(final InputStream stream) {
        try {
            IOUtils.closeQuietly(stream);
        } catch (ProcessingException e) {
            // Jersey client will throw this when closing a stream that is in an invalid state
            LOG.debug("Error closing stream", e);
        }
    }
}
