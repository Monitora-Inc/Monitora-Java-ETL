package etl;

import java.util.*;

import static etl.JiraIssueCreator.criarAlertaCritico;
import static etl.JiraIssueCreator.criarAlertaAtencao;

public class Transformar {
    private Map<String, Integer> limitesCriticos;
    private Map<String, Integer> limitesAtencao;

    public Transformar(Map<String, Integer> limitesCriticos, Map<String, Integer> limitesAtencao) {
        this.limitesCriticos = limitesCriticos;
        this.limitesAtencao = limitesAtencao;
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

            // pula a linha se não houver 8 colunas
            if (col == null || col.length < 8)
                continue;

            // ignora linha de cabeçalho
            if (col[0] == null || "id".equalsIgnoreCase(col[0]))
                continue;

            boolean temNulo = false;
            for (int i = 0; i < 8; i++) {
                if (col[i] == null || col[i].trim().isEmpty()) {
                    temNulo = true;
                    break;
                }
            }
            if (temNulo == true)
                continue;

            String id = col[0].trim();
            String timestamp = col[1].trim();// trim remove espaços
            double cpu = parseDouble(col[2]);
            double ram = parseDouble(col[3]);
            double disco = parseDouble(col[4]);
            double usoRede = parseDouble(col[5]);
            double latencia = parseDouble(col[6]);
            int qtdProcessos = (int) parseDouble(col[7]);

            // busca limites do banco mas se não houver define valores padrao
            int limiteCpuCritico = limitesCriticos.getOrDefault("CPU", 80);
            int limiteCpuAtencao = limitesAtencao.getOrDefault("CPU", 70);

            int limiteRamCritico = limitesCriticos.getOrDefault("RAM", 85);
            int limiteRamAtencao = limitesAtencao.getOrDefault("RAM", 75);

            int limiteDiscoCritico = limitesCriticos.getOrDefault("DISCO", 90);
            int limiteDiscoAtencao = limitesAtencao.getOrDefault("DISCO", 80);

            String statusCpu = getNivelAlerta(cpu, limiteCpuAtencao, limiteCpuCritico);
            if (cpu >= limiteCpuCritico) {
                contadorForaLimiteCriticoCPU++;
                if ((contadorForaLimiteCriticoCPU == 5
                        || contadorForaLimiteCriticoCPU + contadorForaLimiteAtencaoCPU == 5)
                                && contadorForaLimiteCriticoCPU > 0) {
                    criarAlertaCritico(id, "CPU", timestamp);
                }
            } else if (cpu >= limiteCpuAtencao) {
                contadorForaLimiteAtencaoCPU++;
                if (contadorForaLimiteAtencaoCPU == 5) {
                    criarAlertaAtencao(id, "CPU", timestamp);
                }
            } else {
                contadorForaLimiteCriticoCPU = 0;
                contadorForaLimiteAtencaoCPU = 0;
            }

            String statusRam = getNivelAlerta(ram, limiteRamAtencao, limiteRamCritico);
            if (ram > limiteRamCritico) {
                contadorForaLimiteCriticoRAM++;
                if (contadorForaLimiteCriticoRAM == 5
                        || contadorForaLimiteCriticoRAM + contadorForaLimiteAtencaoRAM == 5
                                && contadorForaLimiteCriticoRAM > 0) {
                    criarAlertaCritico(id, "Memória RAM", timestamp);
                }
            } else if (ram > limiteRamAtencao) {
                contadorForaLimiteAtencaoRAM++;
                if (contadorForaLimiteAtencaoRAM == 5) {
                    criarAlertaAtencao(id, "Memória RAM", timestamp);
                }
            } else {
                contadorForaLimiteCriticoRAM = 0;
                contadorForaLimiteAtencaoRAM = 0;
            }

            String statusDisco = getNivelAlerta(disco, limiteDiscoAtencao, limiteDiscoCritico);
            if (disco > limiteDiscoCritico) {
                contadorForaLimiteCriticoDisco++;
                if (contadorForaLimiteCriticoDisco == 5
                        || contadorForaLimiteCriticoDisco + contadorForaLimiteAtencaoDisco == 5
                                && contadorForaLimiteCriticoDisco > 0) {
                    criarAlertaCritico(id, "Disco", timestamp);
                }
            } else if (disco > limiteDiscoAtencao) {
                contadorForaLimiteAtencaoDisco++;
                if (contadorForaLimiteAtencaoDisco == 5) {
                    criarAlertaAtencao(id, "Disco", timestamp);
                }
            } else {
                contadorForaLimiteCriticoDisco = 0;
                contadorForaLimiteAtencaoDisco = 0;
            }

            dadosTratados.add(new String[] {
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

    private static String getNivelAlerta(double valor, double limiteAtencao, double limiteCritico) {
        if (valor >= limiteCritico) {
            return "CRITICO";
        } else if (valor >= limiteAtencao) {
            return "ATENCAO";
        } else {
            return "NORMAL";
        }
    }
}
