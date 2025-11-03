package etl;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class Main {

    private static final String URL = "jdbc:mysql://localhost:3306/monitora?useSSL=false&serverTimezone=America/Sao_Paulo";
    private static final String USER = "monitora";
    private static final String SENHA = "monitora@1234";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, SENHA)) {

            //vai procurar o último CSV da pasta
            File pasta = new File("src/Buckets/Raw/");
            //lista os arquivos da pasta que terminam com .csv
            File[] arquivos = pasta.listFiles((dir, nome) -> nome.endsWith(".csv"));


            if (arquivos == null || arquivos.length == 0) {
                throw new RuntimeException("Sem arquivos para ler na pasta " + pasta);
            }

            for (File arquivo : arquivos) {
                System.out.println("Lendo arquivo: " + arquivo.getName());

                // extraindo CSV
                Extrair extrair = new Extrair();
                List<String[]> dados = extrair.extrairDadosCSV(arquivo.getAbsolutePath());

                if (dados == null || dados.isEmpty()) {
                    throw new RuntimeException(
                            "Nenhum dado foi lido. Verifique o caminho ou o conteúdo do arquivo.");
                }

                // data e hora da captura
                String dataCsv = dados.get(1)[1];
                SimpleDateFormat formatoCSV = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date data = formatoCSV.parse(dataCsv);
                SimpleDateFormat dataFormatada = new SimpleDateFormat("dd_MM_yyyy_HH_mm");
                String formattedDate = dataFormatada.format(data);

                // pegando o id do servidor no csv
                String idServidor = dados.get(1)[0];
                System.out.println("ID do servidor do CSV: " + idServidor);

                // buscando limites no banco
                ParametroDAO parametroDAO = new ParametroDAO(conn);
                Map<String, Map<String, Integer>> limites = parametroDAO.buscarLimites(idServidor);
                Map<String, Integer> limitesCriticos = limites.get("critico");
                Map<String, Integer> limitesAtencao = limites.get("atencao");

                // transforma com base nos limites do banco
                Transformar transformar = new Transformar(limitesCriticos, limitesAtencao);
                List<String[]> dadosTransformados = transformar.transformar(dados);

                // gera novo CSV com dados tratados
                Carregar carregar = new Carregar();
                carregar.carregarParaCSV(dadosTransformados,
                        idServidor, formattedDate);
            }

            System.out.println("ETL executada com sucesso!");
        } catch (SQLException e) {
            System.out.println("Erro de conexão: " + e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
