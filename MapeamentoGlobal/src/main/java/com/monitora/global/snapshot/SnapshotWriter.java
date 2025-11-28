package com.monitora.global.snapshot;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.monitora.global.model.MetricaTratada;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class SnapshotWriter {
    private final AmazonS3 s3;

    public SnapshotWriter(AmazonS3 s3) {
        this.s3 = s3;
    }

    public String salvar(String bucket, String key, MetricaTratada t) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("idDataCenter;timestamp;cpu;ram;disco;rede;saude;cor;up;diferenca\n");
        sb.append(t.idDataCenter).append(";")
                .append(t.timestamp).append(";")
                .append(t.cpuPercent).append(";")
                .append(t.ramPercent).append(";")
                .append(t.discoPercent).append(";")
                .append(t.redeMb).append(";")
                .append(t.saude).append(";")
                .append(t.cor).append(";")
                .append(t.up).append(";")
                .append(t.diferenca);

        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(data.length);
        meta.setContentType("text/csv");
        s3.putObject(bucket, key, new ByteArrayInputStream(data), meta);
        return key;
    }
}
