package etl;
import java.sql.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    private static final String URL = "jdbc:mysql://localhost:3306/monitora?useSSL=false&serverTimezone=America/Sao_Paulo";
    private static final String USER = "monitora";
    private static final String SENHA = "monitora@1234";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, SENHA)) {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            SimpleDateFormat dataFormatada = new SimpleDateFormat("dd_MM_yyyy_HH_mm");
            String formattedDate = dataFormatada.format(timestamp);

            //extraindo CSV
            Extrair extrair = new Extrair();
            List<String[]> dados = extrair.extrairDadosCSV("src\\csvs\\captura_C0D000.csv");

            //pegando o id do servidor no csv
            int idServidor = Integer.parseInt(dados.get(1)[0]);
            System.out.println("ID do servidor do CSV: " + idServidor);

            //buscando limites no banco
            ParametroDAO parametroDAO = new ParametroDAO(conn);
            Map<String, Integer> limites = parametroDAO.buscarLimites(idServidor);

            // transforma com base nos limites do banco
            Transformar transformar = new Transformar(limites);
            List<String[]> dadosTransformados = transformar.transformar(dados);

            //gerar novo CSV com dados tratados
            Carregar carregar = new Carregar();
            carregar.carregarParaCSV(dadosTransformados, "saidaServer"+ idServidor+ "_"+formattedDate + ".csv");

            System.out.println("ETL executada com sucesso!");
        } catch (SQLException e) {
            System.out.println("Erro de conexão: " + e);
        }
    }
}
