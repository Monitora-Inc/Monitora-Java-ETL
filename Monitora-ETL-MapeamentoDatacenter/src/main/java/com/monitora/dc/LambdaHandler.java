package com.monitora.dc;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import com.monitora.dc.Extrair;
import com.monitora.dc.Transformar;
import com.monitora.dc.Carregar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.io.InputStream;
import java.util.List;

public class LambdaHandler implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    @Override
    public String handleRequest(S3Event event, Context context) {

        try {
            // 1. Validando evento
            if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
                context.getLogger().log("Evento S3 vazio.");
                return "SEM_EVENTO";
            }
            var record = event.getRecords().get(0);

            // 2. Pegar bucket e key
            String trustedBucket = record.getS3().getBucket().getName();
            String trustedKey = record.getS3().getObject().getKey();
            context.getLogger().log("Arquivo recebido no TRUSTED: " + trustedBucket + "/" + trustedKey);

            // 3. Variáveis de ambiente
            String clientBucket = System.getenv("CLIENT_BUCKET");
            String dbHost = System.getenv("DB_HOST");
            String dbPort = System.getenv("DB_PORT");
            String dbUser = System.getenv("DB_USER");
            String dbPass = System.getenv("DB_PASS");
            String dbName = System.getenv("DB_NAME");
            if (clientBucket == null || clientBucket.isBlank()) {
                throw new RuntimeException("Variável CLIENT_BUCKET não configurada.");
            }

            // 4. Csv do trusted
            S3Object s3Object = s3.getObject(trustedBucket, trustedKey);
            InputStream csvInputStream = s3Object.getObjectContent();

            // 5. Informações dos caminhos
            String[] partes = trustedKey.split("/");
            String idEmpresaPath = partes[0];
            String uuidServidor = partes[1];
            String ano = partes[2];
            String mes = partes[3];
            String dia = partes[4];
            String nomeArquivo = partes[5];
            context.getLogger().log("Lendo SERVER UUID: " + uuidServidor);

            // 6. Conectação com banco
            String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=America/Sao_Paulo";
            Connection conn = DriverManager.getConnection(url, dbUser, dbPass);
            context.getLogger().log("Conexão com MySQL OK.");

            // 7. Buscar id do datacenter no banco
            ServidorDAO dao = new ServidorDAO(conn);
            Integer idDataCenter = dao.buscarDataCenterPorServidor(uuidServidor);
            if (idDataCenter == null) {
                throw new RuntimeException("Servidor não encontrado no banco: " + uuidServidor);
            }
            context.getLogger().log("Servidor pertence ao DC: " + idDataCenter);

            // 8. Diretório do client
            String diretorioClient =
                    idEmpresaPath + "/" +
                            idDataCenter + "/snapshots/";
            context.getLogger().log("Prefixo de escrita no client: " + diretorioClient);

            // PARTE SEGUINTE: EXTRACT, TRANSFORM E SNAPSHOT

        } catch (Exception e) {
            context.getLogger().log("ERRO NA LAMBDA: " + e.getMessage());
            e.printStackTrace();
            return "ERRO: " + e.getMessage();
        }

        return "OK";
    }
}