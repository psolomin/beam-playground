package com.psolomin.consumer;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Spark and parquet-tools don't like : symbol in file names.
 */
public class NoColonFileNaming implements FileIO.Write.FileNaming {
    private final FileIO.Write.FileNaming defaultNaming;

    public NoColonFileNaming(String runId) {
        this.defaultNaming = FileIO.Write.defaultNaming("out-" + runId, ".parquet");
    }

    @Override
    @NonNull
    public String getFilename(
            @NonNull BoundedWindow window,
            @NonNull PaneInfo pane,
            int numShards,
            int shardIndex,
            @NonNull Compression compression) {
        String defaultName = defaultNaming.getFilename(window, pane, numShards, shardIndex, compression);
        return defaultName.replace(":", "-");
    }
}
