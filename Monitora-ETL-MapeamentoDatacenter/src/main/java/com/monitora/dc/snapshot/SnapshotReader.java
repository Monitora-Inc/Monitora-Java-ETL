package com.monitora.dc.snapshot;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.monitora.dc.model.MetricaTratada;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class SnapshotReader {
    private final AmazonS3 s3;
    public SnapshotReader(AmazonS3 s3) {
        this.s3 = s3;
    }

    public String buscarSnapshotMaisRecente(String bucket, String prefix) {
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix)
                .withDelimiter(null);
        ListObjectsV2Result res = s3.listObjectsV2(req);
        if (res.getObjectSummaries().isEmpty()) {
            return null;
        }
        S3ObjectSummary ultimo = res.getObjectSummaries().stream()
                .max((a, b) -> a.getLastModified().compareTo(b.getLastModified()))
                .orElse(null);
        return ultimo != null ? ultimo.getKey() : null;
    }

    public Map<String, MetricaTratada> lerSnapshot(String bucket, String key) throws Exception {
        Map<String, MetricaTratada> map = new HashMap<>();
        if (key == null) return map;
        S3Object obj = s3.getObject(bucket, key);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(obj.getObjectContent()))) {
            String header = reader.readLine();
            if (header == null) return map;
            String linha;
            while ((linha = reader.readLine()) != null) {
                if (linha.trim().isEmpty()) continue;
                String[] col = linha.split(";");
                if (col.length < 12) continue;
                MetricaTratada mt = new MetricaTratada();
                mt.idServidor = col[0];
                mt.timestamp = col[1];
                mt.cpuPercent = Double.parseDouble(col[2]);
                mt.ramPercent = Double.parseDouble(col[3]);
                mt.discoPercent = Double.parseDouble(col[4]);
                mt.redeMb = Double.parseDouble(col[5]);
                mt.redeMbS = Double.parseDouble(col[6]);
                mt.latenciaMs = Double.parseDouble(col[7]);
                mt.processos = Integer.parseInt(col[8]);
                mt.saude = Double.parseDouble(col[9]);
                mt.up = Boolean.parseBoolean(col[10]);
                mt.cor = col[11];
                map.put(mt.idServidor, mt);
            }
        }
        return map;
    }
}
