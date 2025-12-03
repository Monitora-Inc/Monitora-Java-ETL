package com.monitora.global;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

public class LambdaHandler implements RequestHandler<SNSEvent, String> {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    @Override
    public String handleRequest(SNSEvent event, Context context) {

        try {
            if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
                context.getLogger().log("Evento SNS vazio.");
                return "SEM_EVENTO";
            }

            // -----------------------------------------------------------
            // 1) PEGAR JSON DO SNS
            // -----------------------------------------------------------
            String snsMessage = event.getRecords().get(0).getSNS().getMessage();
            context.getLogger().log("SNS Message recebido: " + snsMessage);

            // -----------------------------------------------------------
            // 2) PARSEAR JSON MANUALMENTE
            // -----------------------------------------------------------
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(snsMessage);

            String trustedBucket = root.get("Records").get(0).get("s3").get("bucket").get("name").asText();
            String trustedKey    = root.get("Records").get(0).get("s3").get("object").get("key").asText();

            context.getLogger().log("Trusted recebido via SNS: " + trustedBucket + "/" + trustedKey);

            // -----------------------------------------------------------
            // 3) PEGAR VARS DE AMBIENTE
            // -----------------------------------------------------------
            String clientBucket = System.getenv("CLIENT_BUCKET");
            String dbHost       = System.getenv("DB_HOST");
            String dbPort       = System.getenv("DB_PORT");
            String dbUser       = System.getenv("DB_USER");
            String dbPass       = System.getenv("DB_PASS");
            String dbName       = System.getenv("DB_NAME");

            if (clientBucket == null || clientBucket.isBlank()) {
                throw new RuntimeException("CLIENT_BUCKET não configurado.");
            }

            // -----------------------------------------------------------
            // 4) LER O ARQUIVO DO BUCKET TRUSTED
            // -----------------------------------------------------------
            S3Object s3Obj = s3.getObject(trustedBucket, trustedKey);
            InputStream input = s3Obj.getObjectContent();

            context.getLogger().log("Lendo arquivo trusted...");

            Extrair extrair = new Extrair();
            List<MetricaBruta> brutas = extrair.extrair(input);

            context.getLogger().log("Linhas extraídas: " + brutas.size());

            if (brutas.isEmpty()) {
                throw new RuntimeException("Trusted sem métricas.");
            }

            // -----------------------------------------------------------
            // 5) PARSE DA CHAVE PARA MONTAR O CAMINHO
            // -----------------------------------------------------------
            String[] partes = trustedKey.split("/");
            if (partes.length < 6) {
                throw new RuntimeException("Trusted key inválida: " + trustedKey);
            }

            String idEmpresa  = partes[0];
            String idServidor = partes[1];
            String ano = partes[2];
            String mes = partes[3];
            String dia = partes[4];

            // -----------------------------------------------------------
            // 6) BUSCAR DATACENTER NO MYSQL
            // -----------------------------------------------------------
            String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=America/Sao_Paulo";

            Integer idDataCenter;
            Double limite;

            try (Connection conn = DriverManager.getConnection(url, dbUser, dbPass)) {
                DataCenterDAO dao = new DataCenterDAO(conn);
                idDataCenter = dao.buscarDataCenterPorServidor(idServidor);

                if (idDataCenter == null) {
                    throw new RuntimeException("Servidor sem datacenter no banco: " + idServidor);
                }

                context.getLogger().log("idDataCenter encontrado: " + idDataCenter);

                limite = dao.buscarLimites(idDataCenter);
                if (limite == null) {
                    throw new RuntimeException("Limites não encontrados para o datacenter: " + idDataCenter);
                }

                context.getLogger().log("Limites encontrados: " + limite);
            }

            // -----------------------------------------------------------
            // 7) TRANSFORMAR DADOS
            // -----------------------------------------------------------
            Transformar transformar = new Transformar();
            MetricaTratada tratada =
                    transformar.transformar(String.valueOf(idDataCenter), brutas, limite);

            // -----------------------------------------------------------
            // 8) MONTAR CAMINHO DO BUCKET DESTINO
            // -----------------------------------------------------------
            String prefix = idEmpresa + "/global/snapshots/" + ano + "/" + mes + "/" + dia + "/";

            String nomeArquivo = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("HH-mm-ss")) + ".csv";

            String keyFinal = prefix + nomeArquivo;

            context.getLogger().log("Salvando snapshot em: " + keyFinal);

            // -----------------------------------------------------------
            // 9) ESCREVER NO CLIENT BUCKET
            // -----------------------------------------------------------
            SnapshotWriter writer = new SnapshotWriter(s3);
            writer.salvar(clientBucket, keyFinal, tratada);

            context.getLogger().log("Snapshot GLOBAL salvo com sucesso.");

            return "OK";

        } catch (Exception e) {
            context.getLogger().log("ERRO: " + e.getMessage());
            e.printStackTrace();
            return "ERRO: " + e.getMessage();
        }
    }
}
