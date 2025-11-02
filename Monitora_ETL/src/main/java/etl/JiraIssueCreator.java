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
    private static final String PROJECT_KEY = "MONA"; //

    public static void criarAlertaAtencao(String idServidor, String componente, String DataHora) throws Exception {
        String mensagem = "ATENÇÃO: "+componente+" - Servidor "+idServidor;
        String prioridade = "High";
        criarChamadoJira(mensagem, prioridade, componente, DataHora);
    }

    public static void criarAlertaCritico(String idServidor, String componente, String DataHora) throws Exception {
        String mensagem = "ALERTA CRÍTICO: "+componente+" - Servidor "+idServidor;
        String prioridade = "Highest";
        criarChamadoJira(mensagem, prioridade, componente, DataHora);
    }

    public static void criarChamadoJira(String mensagem, String prioridade, String componente, String DataHora) throws Exception {
        //Endpoint REST
        URL url = new URL(JIRA_URL + "/rest/api/3/issue");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        //Autenticação (Basic Auth com Base64)
        String authString = USERNAME + ":" + API_TOKEN;
        String encodedAuth = Base64.getEncoder().encodeToString(authString.getBytes());

        //Configuração da Conexão
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true); // Indica que estamos enviando dados no corpo da requisição

        //Corpo da Requisição (Payload JSON)
        String jsonInputString = String.format(
                "{"
                        + "\"fields\": {"
                        + "  \"project\": {"
                        + "    \"key\": \"%s\""
                        + "  },"
                        + "  \"summary\": \""+mensagem+"\"," // Título (Título do alerta que vai aparecer no slack)
                        + "  \"description\": {"
                        + "    \"type\": \"doc\","
                        + "    \"version\": 1,"
                        + "    \"content\": [{"
                        + "      \"type\": \"paragraph\","
                        + "      \"content\": [{"
                        + "        \"text\": \"O componente "+componente+" ultrapassou os parâmetros definidos." +
                        "\\nData e Hora da ocorrência: "+DataHora+"\"," // Descrição (Mensagem que vai aparecer no slack)
                        + "        \"type\": \"text\""
                        + "      }]"
                        + "    }]"
                        + "  },"
                        + "  \"issuetype\": {"
                        + "    \"name\": \"[System] Incident\"" // Tipo de Issue (Task, Bug, Story, etc.)
                        + "  },"
                        + "  \"priority\": {"
                        + "    \"name\": \""+prioridade+"\"" // Prioridade da Issue (Highest = alerta critico, High = alerta atenção)
                        + "  }"
                        + "}"
                        + "}",
                PROJECT_KEY
        );

        //Envio dos Dados
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        //Leitura da Resposta
        int responseCode = conn.getResponseCode();
        System.out.println("Código de Resposta: " + responseCode);

        // Se o chamado foi criado com sucesso (código 201)
        if (responseCode == HttpURLConnection.HTTP_CREATED) {
            Scanner scanner = new Scanner(conn.getInputStream());
            String response = scanner.useDelimiter("\\A").next();
            scanner.close();
            System.out.println("Chamado criado com sucesso! Detalhes:");
            System.out.println(response);
        } else {
            // Leitura da mensagem de erro se o código não for 201
            Scanner scanner = new Scanner(conn.getErrorStream());
            String errorResponse = scanner.useDelimiter("\\A").next();
            scanner.close();
            System.err.println("Falha ao criar chamado. Resposta de Erro:");
            System.err.println(errorResponse);
        }

        conn.disconnect();
    }
}
