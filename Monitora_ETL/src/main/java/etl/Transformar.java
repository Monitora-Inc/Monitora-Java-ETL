package etl;

import java.util.*;

public class Transformar {
    private Map<String, Integer> limites;

    public Transformar(Map<String, Integer> limites) {
        this.limites = limites;
    }

    public List<String[]> transformar(List<String[]> dadosBrutos) {
        List<String[]> dadosTratados = new ArrayList<>();

        for (String[] col : dadosBrutos) {
            //pula a linha se não houver 8 colunas ou se for o cabeçalho
            if (col.length < 8 || col[0].equalsIgnoreCase("id")) continue;

            int id =(int) parseDouble(col[0]);
            String timestamp = col[1].trim();//trim remove espaços
            double cpu = parseDouble(col[2]);
            double ram = parseDouble(col[3]);
            double disco = parseDouble(col[4]);
            double usoRede = parseDouble(col[5]);
            double latencia = parseDouble(col[6]);
            int qtdProcessos = (int) parseDouble(col[7]);

            //busca limites do banco mas se não houver define valores padrao
            int limiteCpu = limites.getOrDefault("CPU", 80);
            int limiteRam = limites.getOrDefault("RAM", 85);
            int limiteDisco = limites.getOrDefault("DISCO", 90);

            String statusCpu = getNivelAlerta(cpu, limiteCpu);
            String statusRam = getNivelAlerta(ram, limiteRam);
            String statusDisco = getNivelAlerta(disco, limiteDisco);

            dadosTratados.add(new String[]{
                    String.valueOf(id), timestamp,
                    String.valueOf(cpu),
                    String.valueOf(ram),
                    String.valueOf(disco),
                    String.valueOf(usoRede),
                    String.valueOf(latencia),
                    String.valueOf(qtdProcessos),
                    statusCpu, statusRam, statusDisco
            });
        }

        return dadosTratados;
    }

    private static double parseDouble(String s) {
        try {
            return Double.parseDouble(s.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static String getNivelAlerta(double valor, double limite) {
        if (valor >= limite) return "ALERTA";
        else return "NORMAL";
    }
}
