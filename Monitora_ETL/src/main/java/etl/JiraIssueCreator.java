package etl;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Scanner;

public class JiraIssueCreator {

    // --- Configurações ---
    private static final String JIRA_URL = "https://sptech-team-lpyjf1yr.atlassian.net";
    private static final String API_TOKEN = "";
    private static final String USERNAME = "monitora373@gmail.com";
    private static final String PROJECT_KEY = "MONA";

    public static void criarAlertaAtencao(String idServidor, String componente, String DataHora, double valorCapturado, String parametro, String unidadeMedida) throws Exception {
        String mensagem = "ATENÇÃO: "+componente+" - Servidor "+idServidor;
        String prioridade = "High";
        criarChamadoJira(mensagem, prioridade, componente, DataHora, idServidor, valorCapturado, parametro, unidadeMedida);
    }

    public static void criarAlertaCritico(String idServidor, String componente, String DataHora, double valorCapturado, String parametro, String unidadeMedida) throws Exception {
        String mensagem = "ALERTA CRÍTICO: "+componente+" - Servidor "+idServidor;
        String prioridade = "Highest";
        criarChamadoJira(mensagem, prioridade, componente, DataHora, idServidor, valorCapturado, parametro, unidadeMedida);
    }

    public static void criarChamadoJira(String mensagem, String prioridade, String componente, String DataHora, String idServidor, double valorCapturado, String parametro, String unidadeMedida) throws Exception {
        URL url = new URL(JIRA_URL + "/rest/api/3/issue");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        String authString = USERNAME + ":" + API_TOKEN;
        String encodedAuth = Base64.getEncoder().encodeToString(authString.getBytes());

        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String jsonInputString = String.format("{"
                        + "\"fields\": {"
                        + "  \"project\": {"
                        + "    \"key\": \"%s\""
                        + "  },"
                        + "  \"summary\": \"" + mensagem + "\","
                        + "  \"description\": {"
                        + "    \"type\": \"doc\","
                        + "    \"version\": 1,"
                        + "    \"content\": [{"
                        + "      \"type\": \"paragraph\","
                        + "      \"content\": [{"
                        + "        \"text\": \"O componente " + componente + " ultrapassou os parâmetros definidos."
                        + "\\nData e Hora da ocorrência: " + DataHora
                        + "\\nServidor: " + idServidor
                        + "\\nValor capturado: " + valorCapturado + unidadeMedida
                        + "\\nParâmetro definido: " + parametro + unidadeMedida + "\","
                        + "        \"type\": \"text\""
                        + "      }]"
                        + "    }]"
                        + "  },"
                        + "  \"issuetype\": {"
                        + "    \"name\": \"[System] Incident\""
                        + "  },"
                        + "  \"priority\": {"
                        + "    \"name\": \"" + prioridade + "\""
                        + "  },"
                        + "  \"customfield_10010\": \"240\""
                        + "}"
                        + "}",
                PROJECT_KEY);

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();
        System.out.println("Código de Resposta: " + responseCode);

        if (responseCode == HttpURLConnection.HTTP_CREATED) {
            Scanner scanner = new Scanner(conn.getInputStream());
            String response = scanner.useDelimiter("\\A").next();
            scanner.close();
            System.out.println("Chamado criado com sucesso! Detalhes:");
            System.out.println(response);
        } else {
            Scanner scanner = new Scanner(conn.getErrorStream());
            String errorResponse = scanner.useDelimiter("\\A").next();
            scanner.close();
            System.err.println("Falha ao criar chamado. Resposta de Erro:");
            System.err.println(errorResponse);
        }

        conn.disconnect();
    }
}
