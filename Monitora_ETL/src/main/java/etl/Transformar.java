package etl;

import java.util.*;

import static etl.JiraIssueCreator.criarAlertaCritico;
import static etl.JiraIssueCreator.criarAlertaAtencao;

public class Transformar {
    private Map<String, Integer> limites;

    public Transformar(Map<String, Integer> limites) {
        this.limites = limites;
    }

    public List<String[]> transformar(List<String[]> dadosBrutos) throws Exception {
        List<String[]> dadosTratados = new ArrayList<>();

        int contadorForaLimiteCriticoCPU = 0;
        int contadorForaLimiteAtencaoCPU = 0;
        int contadorForaLimiteCriticoRAM = 0;
        int contadorForaLimiteAtencaoRAM = 0;
        int contadorForaLimiteCriticoDisco = 0;
        int contadorForaLimiteAtencaoDisco = 0;

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
            if (cpu > limiteCpu) {
                contadorForaLimiteCriticoCPU++;
                if (contadorForaLimiteCriticoCPU == 5 || contadorForaLimiteCriticoCPU + contadorForaLimiteAtencaoCPU == 5
                        && contadorForaLimiteCriticoCPU > 0) {
                    criarAlertaCritico(id,"CPU",timestamp);
                }
            } else if (cpu > (limiteCpu - 10)) {
                contadorForaLimiteAtencaoCPU++;
                if (contadorForaLimiteAtencaoCPU == 5) {
                    criarAlertaAtencao(id,"CPU",timestamp);
                }
            } else {
                contadorForaLimiteCriticoCPU = 0;
                contadorForaLimiteAtencaoCPU = 0;
            }

            String statusRam = getNivelAlerta(ram, limiteRam);
            if (ram > limiteRam) {
                contadorForaLimiteCriticoRAM++;
                if (contadorForaLimiteCriticoRAM == 5 || contadorForaLimiteCriticoRAM + contadorForaLimiteAtencaoRAM == 5
                        && contadorForaLimiteCriticoRAM > 0) {
                    criarAlertaCritico(id,"Memória RAM",timestamp);
                }
            } else if (ram > (limiteRam - 10)) {
                contadorForaLimiteAtencaoRAM++;
                if (contadorForaLimiteAtencaoRAM == 5) {
                    criarAlertaAtencao(id,"Memória RAM",timestamp);
                }
            } else {
                contadorForaLimiteCriticoRAM = 0;
                contadorForaLimiteAtencaoRAM = 0;
            }

            String statusDisco = getNivelAlerta(disco, limiteDisco);
            if (disco > limiteDisco) {
                contadorForaLimiteCriticoDisco++;
                if (contadorForaLimiteCriticoDisco == 5 || contadorForaLimiteCriticoDisco + contadorForaLimiteAtencaoDisco == 5
                        && contadorForaLimiteCriticoDisco > 0) {
                    criarAlertaCritico(id,"Disco",timestamp);
                }
            } else if (disco > (limiteDisco - 10)) {
                contadorForaLimiteAtencaoDisco++;
                if (contadorForaLimiteAtencaoDisco == 5) {
                    criarAlertaAtencao(id,"Disco",timestamp);
                }
            } else {
                contadorForaLimiteCriticoDisco = 0;
                contadorForaLimiteAtencaoDisco = 0;
            }

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
