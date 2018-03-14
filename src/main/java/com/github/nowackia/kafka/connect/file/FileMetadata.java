package com.github.nowackia.kafka.connect.file;

/**
 * Metadata for storing information on the processed files
 */
public class FileMetadata {
    private final String path;
    private final long length;

    public FileMetadata(String path, long length) {
        this.path = path;
        this.length = length;
    }

    public String getPath() {
        return path;
    }

    public long getLen() {
        return length;
    }

    @Override
    public String toString() {
        return String.format("[path = %s, length = %s]", path, length);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof FileMetadata)) return false;

        FileMetadata metadata = (FileMetadata) object;

        if (this.path.equals(metadata.getPath()))
            return true;
        if (this.length == metadata.getLen())
            return true;

        return false;
    }

    public int hashCode() {
        return path == null ? 0 : path.hashCode();
    }
}
