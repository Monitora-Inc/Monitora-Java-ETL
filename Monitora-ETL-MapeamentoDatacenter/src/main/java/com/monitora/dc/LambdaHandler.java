package com.monitora.dc;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.monitora.dc.dao.ServidorDAO;
import com.monitora.dc.model.MetricaBruta;
import com.monitora.dc.model.MetricaTratada;
import com.monitora.dc.snapshot.SnapshotReader;
import com.monitora.dc.snapshot.SnapshotWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LambdaHandler implements RequestHandler<SNSEvent, String> {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    @Override
    public String handleRequest(SNSEvent event, Context context) {

        try {
            // 1) Validando evento SNS
            if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
                context.getLogger().log("Evento SNS vazio.");
                return "SEM_EVENTO";
            }

            // 2) Pegando JSON da mensagem SNS
            String snsMessage = event.getRecords().get(0).getSNS().getMessage();
            context.getLogger().log("SNS Message recebido: " + snsMessage);

            // 3) Parsear JSON manualmente
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(snsMessage);

            String trustedBucket = root.get("Records").get(0).get("s3").get("bucket").get("name").asText();
            String trustedKey    = root.get("Records").get(0).get("s3").get("object").get("key").asText();

            context.getLogger().log("Arquivo recebido no TRUSTED via SNS: " 
                + trustedBucket + "/" + trustedKey);

            // 4) Variáveis de ambiente
            String clientBucket = System.getenv("CLIENT_BUCKET");
            String dbHost       = System.getenv("DB_HOST");
            String dbPort       = System.getenv("DB_PORT");
            String dbUser       = System.getenv("DB_USER");
            String dbPass       = System.getenv("DB_PASS");
            String dbName       = System.getenv("DB_NAME");

            if (clientBucket == null || clientBucket.isBlank()) {
                throw new RuntimeException("CLIENT_BUCKET não configurado.");
            }

            // 5) Ler o CSV do trusted
            S3Object s3Object = s3.getObject(trustedBucket, trustedKey);
            InputStream csvInputStream = s3Object.getObjectContent();
            context.getLogger().log("Lendo CSV do trusted...");

            // 6) Extrair caminhos
            String[] partes = trustedKey.split("/");
            String idEmpresaPath = partes[0];
            String uuidServidor  = partes[1];
            String ano           = partes[2];
            String mes           = partes[3];
            String dia           = partes[4];

            // 7) Conexão MySQL
            String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=America/Sao_Paulo";
            try (Connection conn = DriverManager.getConnection(url, dbUser, dbPass)) {

                context.getLogger().log("Conexão com MySQL OK.");

                // 8) Buscar datacenter
                ServidorDAO dao = new ServidorDAO(conn);
                Integer idDataCenter = dao.buscarDataCenterPorServidor(uuidServidor);
                if (idDataCenter == null) {
                    throw new RuntimeException("Servidor não encontrado no banco: " + uuidServidor);
                }
                context.getLogger().log("Servidor pertence ao DC: " + idDataCenter);

                // 9) Prefixo no bucket client
                String diretorioClient =
                        idEmpresaPath + "/" + idDataCenter + "/snapshots/" 
                        + ano + "/" + mes + "/" + dia + "/";
                context.getLogger().log("Prefixo final: " + diretorioClient);

                // 10) Extrair CSV
                Extrair extrair = new Extrair();
                List<MetricaBruta> brutas = extrair.extrair(csvInputStream);
                context.getLogger().log("Linhas extraídas: " + brutas.size());

                // 11) Transformar métricas
                Transformar transformar = new Transformar();
                Map<String, MetricaTratada> novasMetricas = new HashMap<>();

                for (MetricaBruta b : brutas) {
                    MetricaTratada t = transformar.transformar(b);
                    novasMetricas.put(t.idServidor, t);
                }

                // 12) Ler snapshot anterior
                SnapshotReader reader = new SnapshotReader(s3);
                String snapshotAntigoKey = reader.buscarSnapshotMaisRecente(clientBucket, diretorioClient);

                Map<String, MetricaTratada> mapa =
                        (snapshotAntigoKey == null)
                                ? new HashMap<>()
                                : reader.lerSnapshot(clientBucket, snapshotAntigoKey);

                // 13) Mesclar métricas novas
                mapa.putAll(novasMetricas);

                // 14) Escrever novo snapshot
                SnapshotWriter writer = new SnapshotWriter(s3);
                String novoSnapshotKey =
                        writer.salvarSnapshot(clientBucket, diretorioClient, mapa.values());

                context.getLogger().log("Snapshot DC salvo em: " + novoSnapshotKey);

                return "OK";
            }

        } catch (Exception e) {
            context.getLogger().log("ERRO NA LAMBDA: " + e.getMessage());
            e.printStackTrace();
            return "ERRO: " + e.getMessage();
        }
    }
}
