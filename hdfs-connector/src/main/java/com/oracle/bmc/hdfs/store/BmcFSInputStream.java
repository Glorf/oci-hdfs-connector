/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;

import com.oracle.bmc.hdfs.util.FSStreamUtils;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import com.google.common.base.Supplier;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.hdfs.store.BmcInstrumentation;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

/**
 * Common implementation of {@link FSInputStream} that has basic read support, along with state validation.
 * Implementations should inherit from this class when there is not too much custom logic required to implement seek
 * behavior.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
abstract class BmcFSInputStream extends FSInputStream {
    private static final int EOF = -1;
    private static final int readahead = 64 * 1024; //64KB

    private final ObjectStorage objectStorage;
    private final FileStatus status;
    private final Supplier<GetObjectRequest.Builder> requestBuilder;

    @Getter(value = AccessLevel.PROTECTED)
    private final Statistics statistics;

    @Setter(value = AccessLevel.PROTECTED)
    @Getter(value = AccessLevel.PROTECTED)
    private InputStream sourceInputStream;

    private long currentPosition = 0;
    private long nextPosition = 0;

    private boolean closed = false;

    @Override
    public long getPos() throws IOException {
        return (nextPosition<0)? 0: nextPosition;
    }

    @Override
    public void seek(final long position) throws IOException {
        LOG.debug("SEEK Called to {}", position);
        validateState(position);
        if (remainingInCurrentRequest() <= 0) {
            return;
        }

        if (position < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }

        if (position >= this.status.getLen()) {
            throw new EOFException("Cannot seek to " + position + " (file size : " + this.status.getLen() + ")");
        }

        //Perform lazy seek
        nextPosition = position;
        LOG.debug("Requested seek to {}, ended at position {}", position, currentPosition);
    }

    /**
     * Perform the requested seek operation. Note, if the subclass changes the input stream or closes it, a new one must
     * be provided and set using {@link #setSourceInputStream(InputStream)} before returning. The input stream that was
     * originally created (and wrapped by {@link #wrap(InputStream)} can be obtained from
     * {@link #getSourceInputStream()}.
     *
     * @param position
     *            The position to seek to.
     * @return The new position after seeking.
     * @throws IOException
     *             if the operation could not be completed
     */
    abstract protected long doSeek(long position) throws IOException;

    /**
     * There are no new sources, this method always returns false.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public boolean seekToNewSource(final long arg0) throws IOException {
        // no new sources
        return false;
    }

    public long remainingInCurrentRequest() {
        return this.status.getLen() - this.currentPosition;
    }

    private void seekInStream(long targetPos) throws IOException {
        if (sourceInputStream == null) {
            return;
        }
        // compute how much more to skip
        long diff = targetPos - currentPosition;


        if (diff > 0) {
            // forward seek -this is where data can be skipped

            int available = sourceInputStream.available();
            // always seek at least as far as what is available
            long forwardSeekRange = Math.max(readahead, available);
            // work out how much is actually left in the stream
            // then choose whichever comes first: the range or the EOF
            long remainingInCurrentRequest = remainingInCurrentRequest();

            long forwardSeekLimit = Math.min(remainingInCurrentRequest,
                    forwardSeekRange);
            boolean skipForward = remainingInCurrentRequest > 0
                    && diff < forwardSeekLimit;
            if (skipForward) {
                // the forward seek range is within the limits
                long skipped = sourceInputStream.skip(diff);
                if (skipped > 0) {
                    currentPosition += skipped;
                    // as these bytes have been read, they are included in the counter
                    //incrementBytesRead(diff);
                }

                if (currentPosition == targetPos) {
                    // all is well
                    LOG.debug("Now at {}: bytes remaining in file: {}",
                            currentPosition, remainingInCurrentRequest());
                    return;
                } else {
                    // log a warning; continue to attempt to re-open
                    LOG.debug("Failed to seek on {} to {}. Current position {}",
                            status.getPath(), targetPos,  currentPosition);
                }
            }
        } else if (diff < 0) {
            //Failed to perform readahead seek, fallback to old seeker
            LOG.debug("Don't do it: from {} to {}", currentPosition, targetPos);
        } else {
            // targetPos == pos
            if (remainingInCurrentRequest() >= 0) {
                // if there is data left in the stream, or EOF, keep going
                return;
            }

        }

        LOG.debug("Don't do it: {} {}", targetPos, currentPosition);

        // if the code reaches here, the stream needs to be reopened.
        // close the stream; if read the object will be opened at the new pos
        currentPosition = targetPos;
        nextPosition = targetPos;
        IOUtils.closeQuietly(sourceInputStream);
        sourceInputStream = null;
    }

    private void lazySeek(long targetPos) throws IOException {
        long startTime = System.nanoTime();

        //For lazy seek
        seekInStream(targetPos);

        //re-open at specific location if needed
        if (sourceInputStream == null) {
            validateState(targetPos);
        }

        long endTime = System.nanoTime();
        long elapsedTimeMicrosec = (endTime - startTime) / 1000L;
        BmcInstrumentation.incrementTimeElapsedSeekOps(elapsedTimeMicrosec);
        BmcInstrumentation.incrementSeekCalls(1);
    }




    @Override
    public int read() throws IOException {
        validateState(nextPosition);
        try {
            lazySeek(nextPosition);
        } catch (EOFException e) {
            return -1;
        }

        long startTime = System.nanoTime();
        // System.err.println(("INTRUMENTATION: Point 2! " + System.nanoTime()));
        final int byteRead = this.sourceInputStream.read();
        long endTime = System.nanoTime();
        long elapsedTimeMicrosec = (endTime - startTime) / 1000L;

        if (byteRead != EOF) {
            this.currentPosition++;
            this.nextPosition++;
            this.statistics.incrementBytesRead(1L);
        }

        BmcInstrumentation.incrementReadCalls(1);
        BmcInstrumentation.incrementBytesRead((long) byteRead);
        BmcInstrumentation.incrementTimeElapsedReadOps(elapsedTimeMicrosec);

        return byteRead;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        LOG.debug("Request from {} to next {} with offset {} and length {}", this.currentPosition, this.nextPosition, off, len);
        validateState(nextPosition);

        try {
            lazySeek(nextPosition);
        } catch (EOFException e) {
            return -1;
        }

        long startTime = System.nanoTime();
        //System.err.println(("INTRUMENTATION: Point 3! " + System.nanoTime()));
        final int bytesRead = this.sourceInputStream.read(b, off, len);
        long endTime = System.nanoTime();
        long elapsedTimeMicrosec = (endTime - startTime) / 1000L;

        if (bytesRead != EOF) {
            this.currentPosition += bytesRead;
            this.nextPosition += bytesRead;
            this.statistics.incrementBytesRead(bytesRead);
            LOG.debug("Read {} bytes", bytesRead);
        }

        BmcInstrumentation.incrementReadCalls(1);
        BmcInstrumentation.incrementBytesRead((long) bytesRead);
        BmcInstrumentation.incrementTimeElapsedReadOps(elapsedTimeMicrosec);

        return bytesRead;
    }

    @Override
    public int available() throws IOException {
        this.validateState(this.currentPosition);

        final long bytesRemaining = this.status.getLen() - this.currentPosition;
        LOG.debug("Bytes available: {}", bytesRemaining);
        return bytesRemaining <= Integer.MAX_VALUE ? (int) bytesRemaining : Integer.MAX_VALUE;
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.closed = true;
        if (this.sourceInputStream != null) {
            // specifications says close should not throw any IOExceptions
            FSStreamUtils.closeQuietly(this.sourceInputStream);
            this.sourceInputStream = null;
        }
    }

    /**
     * Allows the subclass to wrap the raw input stream from Casper in another one if desired.
     *
     * @param rawInputStream
     *            The raw input stream.
     * @return An input stream to set as the source.
     * @throws IOException
     *             if the operation could not be completed.
     */
    protected InputStream wrap(final InputStream rawInputStream) throws IOException {
        return new BufferedInputStream(rawInputStream, readahead);
    }

    /**
     * Allows subclasses to validate the state of this stream. Involves:
     * <ol>
     * <li>Verifying the stream is not closed.</li>
     * <li>Creating a new input stream (and wrapping it with {@link #wrap(InputStream)})</li>
     * </ol>
     *
     * @param startPosition
     *            The starting byte offset.
     * @throws IOException
     *             if the filesystem could not be initialized
     */
    protected void validateState(final long startPosition) throws IOException {
        this.checkNotClosed();
        try {
            this.verifyInitialized(startPosition);
        } catch (final BmcException e) {
            throw new IOException("Unable to initialize data", e);
        }
    }

    private void checkNotClosed() throws IOException {
        if (this.closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    private void verifyInitialized(final long startPosition) throws IOException, BmcException {
        if (this.sourceInputStream != null) {
            return;
        }

        LOG.debug("Initializing with start position {}", startPosition);
        // end is null as we want until the end of object
        Range range = new Range(startPosition, null);

        GetObjectRequest request = this.requestBuilder.get().range(range).build();

        final GetObjectResponse response = this.objectStorage.getObject(request);
        LOG.debug(
                "Opened object with etag {} and size {}",
                response.getETag(),
                response.getContentLength());
        final InputStream dataStream = response.getInputStream();

        this.sourceInputStream = this.wrap(dataStream);
        // if range request, use the first byte returned, else it's just 0 (startPosition)
        this.currentPosition = this.nextPosition = response.getContentRange().getStartByte();
    }
}
