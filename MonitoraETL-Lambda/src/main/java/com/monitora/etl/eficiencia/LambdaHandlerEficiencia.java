package com.monitora.etl.eficiencia;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.event.S3EventNotification;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class LambdaHandlerEficiencia implements RequestHandler<SNSEvent, String> {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    private static final String CLIENT = System.getenv("CLIENT_BUCKET");

    @Override
    public String handleRequest(SNSEvent event, Context ctx) {

        try {
            ctx.getLogger().log("SNS recebido. Convertendo para evento S3...\n");

            String snsMsg = event.getRecords().get(0).getSNS().getMessage();
            S3EventNotification s3Event = S3EventNotification.parseJson(snsMsg);

            var rec = s3Event.getRecords().get(0);

            String bucket = rec.getS3().getBucket().getName();
            String key = rec.getS3().getObject().getKey();

            ctx.getLogger().log("Processando arquivo: " + bucket + "/" + key + "\n");

            S3Object obj = s3.getObject(bucket, key);
            InputStream is = obj.getObjectContent();

            // ETL
            ExtrairEficiencia extrair = new ExtrairEficiencia();
            RegistroEficiencia dados = extrair.processar(is, key);

            TransformarEficiencia t = new TransformarEficiencia();
            t.transformar(dados);

            // Monta nome final
            String horaMinuto = dados.hora + "-" + dados.minuto;

            String outKey = String.format(
                    "%s/eficiencia_entrega/%s/%s/%s/%s/%s.csv",
                    dados.empresaId,
                    dados.uuidServidor,
                    dados.data.substring(0, 4),
                    dados.data.substring(5, 7),
                    dados.data.substring(8),
                    horaMinuto
            );

            // CSV final
            String csv = gerarCsv(dados);
            byte[] bytes = csv.getBytes();

            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/csv");

            s3.putObject(new PutObjectRequest(
                    CLIENT,
                    outKey,
                    new ByteArrayInputStream(bytes),
                    meta
            ));

            ctx.getLogger().log("SALVO EM: " + CLIENT + "/" + outKey + "\n");

            return "OK";

        } catch (Exception e) {
            ctx.getLogger().log("Erro na Lambda: " + e.getMessage() + "\n");
            e.printStackTrace();
            return "ERRO";
        }
    }

    private String gerarCsv(RegistroEficiencia r) {
        return
                "servidor_id;data;minutos_totais;minutos_funcionando;disponibilidade;" +
                        "cache_hit_total;cache_miss_total;eficiencia;latencia_media;" +
                        "p50;p90;p99;latencia_nota;saude;total_requisicoes\n" +

                        r.uuidServidor + ";" +
                        r.data + ";" +
                        r.minutosTotais + ";" +
                        r.minutosFuncionando + ";" +
                        r.disponibilidade + ";" +
                        r.cacheHit + ";" +
                        r.cacheMiss + ";" +
                        r.eficiencia + ";" +
                        r.latMedia + ";" +
                        r.p50 + ";" +
                        r.p90 + ";" +
                        r.p99 + ";" +
                        r.latenciaNota + ";" +
                        r.saude + ";" +
                        r.totalRequisicoes +
                        "\n";
    }
}
