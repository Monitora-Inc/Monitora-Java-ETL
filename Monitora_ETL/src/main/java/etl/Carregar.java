package etl;

import java.io.*;
import java.util.List;

public class Carregar {

    public void carregarParaCSV(List<String[]> dados, String caminhoSaida) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(caminhoSaida))) {

            writer.write("id;timestamp;cpu;ram;disco;usoRede;latencia;qtdProcessos;statusCpu;statusRam;statusDisco;statusUsoRede;statusLatencia");
            writer.newLine();

            for (String[] linha : dados) {
                writer.write(String.join(";", linha));
                writer.newLine();
            }

            System.out.println("Arquivo CSV criado com sucesso em: " + caminhoSaida);
        } catch (IOException e) {
            System.out.println("Erro ao criar o arquivo CSV: " + e);
        }
    }
}
