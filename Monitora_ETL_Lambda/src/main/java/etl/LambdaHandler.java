package etl;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class LambdaHandler implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    private static final String DB_NAME = "monitora";
    private static final String DB_USER = "monitora";
    private static final String DB_PASSWORD = "monitora@1234";

    @Override
    public String handleRequest(S3Event event, Context context) {
        try {
            if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
                context.getLogger().log("Evento vazio.\n");
                return "Sem records";
            }

            var record = event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            String srcKey    = record.getS3().getObject().getKey();

            context.getLogger().log("Iniciando ETL para arquivo: " + srcBucket + "/" + srcKey + "\n");

            // 1) Ler CSV bruto do RAW (S3)
            S3Object s3Object = s3Client.getObject(srcBucket, srcKey);
            List<String[]> dadosBrutos = lerCSVDoS3(s3Object.getObjectContent());

            if (dadosBrutos == null || dadosBrutos.isEmpty()) {
                context.getLogger().log("CSV sem dados.\n");
                return "CSV vazio";
            }

            // 2) Extrair idServidor e timestamp da primeira linha de dados
            String idServidor = null;
            String timestampStr = null;
            for (String[] linha : dadosBrutos) {
                if (linha == null || linha.length < 2) continue;
                if (linha[0] == null) continue;
                if ("id".equalsIgnoreCase(linha[0].trim())) continue;
                idServidor = linha[0].trim();
                timestampStr = linha[1].trim();
                break;
            }

            if (idServidor == null || timestampStr == null) {
                context.getLogger().log("Não foi possível extrair idServidor/timestamp.\n");
                return "Sem linha de dados válida";
            }

            context.getLogger().log("idServidor: " + idServidor + " | timestamp: " + timestampStr + "\n");

            // 3) Conectar no banco e buscar limites
            try (Connection conn = criarConexaoBanco()) {
                context.getLogger().log("Conectado ao banco com sucesso.\n");

                ParametroDAO parametroDAO = new ParametroDAO(conn);
                Map<String, Map<String, Double>> limites = parametroDAO.buscarLimites(idServidor);

                Map<String, Double> limitesCriticos = limites.get("critico");
                Map<String, Double> limitesAtencao  = limites.get("atencao");

                if (limitesCriticos == null || limitesAtencao == null) {
                    context.getLogger().log("Limites não encontrados para servidor " + idServidor + "\n");
                    return "Sem limites para servidor";
                }

                // 4) Transformar dados
                Transformar transformar = new Transformar(limitesCriticos, limitesAtencao);
                List<String[]> dadosTratados = transformar.transformar(dadosBrutos);

                context.getLogger().log("Transformação concluída. Linhas tratadas: " + dadosTratados.size() + "\n");

                // 5) Gerar CSV tratado em memória
                byte[] csvBytes = gerarCSVTratado(dadosTratados);

                // 6) Salvar no bucket TRUSTED com O MESMO CAMINHO/ARQUIVO do RAW
                String dstBucket = System.getenv().getOrDefault("TRUSTED_BUCKET", "monitora-trusted");
                String dstKey    = srcKey;  // <- mesmo caminho de onde veio

                ObjectMetadata meta = new ObjectMetadata();
                meta.setContentType("text/csv; charset=UTF-8");
                meta.setContentLength(csvBytes.length);

                context.getLogger().log("Gravando CSV tratado em: " + dstBucket + "/" + dstKey + "\n");

                // S3 cria os "diretórios" automaticamente se o prefixo não existir
                s3Client.putObject(dstBucket, dstKey, new ByteArrayInputStream(csvBytes), meta);

                context.getLogger().log("ETL concluída com sucesso.\n");
                return "OK - ETL concluída";
            }

        } catch (Exception e) {
            context.getLogger().log("Erro na LambdaHandler: " + e.getMessage() + "\n");
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            context.getLogger().log(sw.toString());
            throw new RuntimeException(e);
        }
    }

    // Criar conexão com o banco
    private Connection criarConexaoBanco() throws SQLException {
        String host = System.getenv("DB_HOST");
        if (host == null || host.isBlank()) {
            throw new SQLException("Variável de ambiente DB_HOST não configurada.");
        }

        String url = "jdbc:mysql://" + host + ":3306/" + DB_NAME
                + "?useSSL=false"
                + "&allowPublicKeyRetrieval=true"
                + "&connectTimeout=5000"
                + "&socketTimeout=5000"
                + "&serverTimezone=America/Sao_Paulo";

        return DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
    }

    // Ler CSV
    private List<String[]> lerCSVDoS3(InputStream inputStream) {
        List<String[]> dados = new ArrayList<>();
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] cols = line.split(";");
                dados.add(cols);
            }
        } catch (IOException e) {
            throw new RuntimeException("Erro ao ler CSV do S3: " + e.getMessage(), e);
        }
        return dados;
    }

    private byte[] gerarCSVTratado(List<String[]> dados) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(bos, StandardCharsets.UTF_8);
            BufferedWriter writer = new BufferedWriter(osw);

            // Cabeçalho
            writer.write("id;timestamp;cpu;total_ram;ram_usada;ram_percent;ram_quente;ram_fria;disco_percent;"
                    + "bytesEnv;bytesRecb;usoRedeMb;latencia;pacotes_enviados;pacotes_recebidos;pacotes_perdidos;"
                    + "qtd_processos;uptime_segundos;statusCpu;statusRam;statusDisco;statusUsoRede;statusLatencia");
            writer.newLine();

            for (String[] linha : dados) {
                writer.write(String.join(";", linha));
                writer.newLine();
            }

            writer.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Erro ao gerar CSV tratado em memória: " + e.getMessage(), e);
        }
    }
}
