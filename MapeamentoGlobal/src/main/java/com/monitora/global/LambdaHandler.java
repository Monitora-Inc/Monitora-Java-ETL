package com.monitora.global;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.monitora.global.model.MetricaBruta;
import com.monitora.global.model.MetricaTratada;
import com.monitora.global.snapshot.SnapshotWriter;
import com.monitora.global.dao.DataCenterDAO;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class LambdaHandler implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    @Override
    public String handleRequest(S3Event event, Context context) {

        try {
            if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
                context.getLogger().log("Evento S3 vazio.");
                return "SEM_EVENTO";
            }

            var record = event.getRecords().get(0);

            String trustedBucket = record.getS3().getBucket().getName();
            String trustedKey = record.getS3().getObject().getKey();
            context.getLogger().log("Trusted recebido: " + trustedBucket + "/" + trustedKey);

            String clientBucket = System.getenv("CLIENT_BUCKET");
            String dbHost       = System.getenv("DB_HOST");
            String dbPort       = System.getenv("DB_PORT");
            String dbUser       = System.getenv("DB_USER");
            String dbPass       = System.getenv("DB_PASS");
            String dbName       = System.getenv("DB_NAME");

            if (clientBucket == null || clientBucket.isBlank()) {
                throw new RuntimeException("CLIENT_BUCKET não configurado.");
            }

            S3Object s3Obj = s3.getObject(trustedBucket, trustedKey);
            InputStream input = s3Obj.getObjectContent();

            Extrair extrair = new Extrair();
            List<MetricaBruta> brutas = extrair.extrair(input);
            context.getLogger().log("Linhas extraídas: " + brutas.size());

            if (brutas.isEmpty()) {
                throw new RuntimeException("Trusted sem métricas.");
            }

            String[] partes = trustedKey.split("/");
            if (partes.length < 6) {
                throw new RuntimeException("Trusted key inválida: " + trustedKey);
            }

            String idEmpresa = partes[0];
            String idServidor = partes[1];
            String ano = partes[2];
            String mes = partes[3];
            String dia = partes[4];

            String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=America/Sao_Paulo";

            Integer idDataCenter;
            try (Connection conn = DriverManager.getConnection(url, dbUser, dbPass)) {

                DataCenterDAO dao = new DataCenterDAO(conn);
                idDataCenter = dao.buscarDataCenterPorServidor(idServidor);

                if (idDataCenter == null) {
                    throw new RuntimeException("Servidor sem datacenter no banco: " + idServidor);
                }

                context.getLogger().log("idDataCenter encontrado: " + idDataCenter);
            }

            // Transformar métricas brutas → tratada
            Transformar transformar = new Transformar();
            MetricaTratada tratada = transformar.transformar(String.valueOf(idDataCenter), brutas);

            // Prefixo de saída no client/global
            String prefix = idEmpresa + "/global/snapshots/" + ano + "/" + mes + "/" + dia + "/";

            // Nome do arquivo final (ex: 13-59-02.csv)
            String nomeArquivo = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("HH-mm-ss")) + ".csv";

            String keyFinal = prefix + nomeArquivo;

            // Escrever no S3
            SnapshotWriter writer = new SnapshotWriter(s3);
            writer.salvar(clientBucket, keyFinal, tratada);

            context.getLogger().log("Snapshot GLOBAL salvo em: " + keyFinal);

            return "OK";

        } catch (Exception e) {
            e.printStackTrace();
            return "ERRO: " + e.getMessage();
        }
    }
}
