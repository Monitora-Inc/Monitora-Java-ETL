package com.monitora.dc.snapshot;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.monitora.dc.model.MetricaTratada;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

public class SnapshotWriter {
    private final AmazonS3 s3;
    public SnapshotWriter(AmazonS3 s3) {
        this.s3 = s3;
    }

    public String salvarSnapshot(String bucket, String prefix, Collection<MetricaTratada> servidores) throws Exception {
        // 1. Nome do arquivo
        LocalDateTime agora = LocalDateTime.now();
        String nomeArquivo = agora.format(DateTimeFormatter.ofPattern("HH-mm-ss")) + ".csv";
        String keyFinal = prefix + nomeArquivo;

        // 2. Construir conteúdo CSV
        StringBuilder sb = new StringBuilder();
        sb.append("idServidor;timestamp;cpu;ram;disco;redeMB;redeMbS;latencia;processos;saude;up;cor\n");
        for (MetricaTratada m : servidores) {
            sb.append(m.idServidor).append(";")
                    .append(m.timestamp).append(";")
                    .append(m.cpuPercent).append(";")
                    .append(m.ramPercent).append(";")
                    .append(m.discoPercent).append(";")
                    .append(m.redeMb).append(";")
                    .append(m.redeMbS).append(";")
                    .append(m.latenciaMs).append(";")
                    .append(m.processos).append(";")
                    .append(m.saude).append(";")
                    .append(m.up).append(";")
                    .append(m.cor).append("\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/csv");

        // 3. Upload para o S3
        s3.putObject(bucket, keyFinal, new ByteArrayInputStream(bytes), meta);
        return keyFinal;
    }
}