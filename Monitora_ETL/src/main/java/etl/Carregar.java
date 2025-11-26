package etl;

import java.io.*;
import java.util.List;

public class Carregar {

    public void carregarParaCSV(List<String[]> dados, String idServidor, String formattedDate) {
        String dia = formattedDate.substring(0, 2);
        String mes = formattedDate.substring(3, 5);
        String ano = formattedDate.substring(6, 10);
        String hora = formattedDate.substring(11, 13);
        String minutos = formattedDate.substring(14, 16);
        String caminhoPasta = "src\\Buckets\\Trusted\\" + idServidor + "\\" + ano + "\\" + mes;
        File destino = new File(caminhoPasta);
        if (!destino.exists()) {
            destino.mkdirs();
        }

        String caminhoArquivo = caminhoPasta + "\\" + dia + " - " + hora + " - " + minutos + ".csv";
        File arquivo = new File(caminhoArquivo);
        boolean arquivoExiste = arquivo.exists();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(arquivo, true))) {
            if (!arquivoExiste) {
                writer.write("id;timestamp;cpu;total_ram;ram_usada;ram_percent;ram_quente;ram_fria;disco_percent;bytesEnv;bytesRecb;usoRedeMb;latencia;pacotes_enviados;pacotes_recebidos;pacotes_perdidos;qtd_processos;uptime_segundos;statusCpu;statusRam;statusDisco;statusUsoRede;statusLatencia");
                writer.newLine();
            }

            for (String[] linha : dados) {
                writer.write(String.join(";", linha));
                writer.newLine();
            }

            System.out.println("Arquivo CSV criado com sucesso em: " + caminhoPasta);
        } catch (IOException e) {
            System.out.println("Erro ao criar o arquivo CSV: " + e);
        }
    }

}
