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

        int contCPU = 0;
        int contRAM = 0;
        int contDisco = 0;
        int contUsoRede = 0;
        int contLatencia = 0;

        boolean criticoCPU = false;
        boolean criticoRAM = false;
        boolean criticoDisco = false;
        boolean criticoUsoRede = false;
        boolean criticoLatencia = false;

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
            int limiteCpuCritico = limitesCriticos.getOrDefault("CPU%", 90000) / 1000;
            int limiteCpuAtencao = limitesAtencao.getOrDefault("CPU%", 80000) / 1000;

            int limiteRamCritico = limitesCriticos.getOrDefault("RAM%", 85000) / 1000;
            int limiteRamAtencao = limitesAtencao.getOrDefault("RAM%", 75000) / 1000;

            int limiteDiscoCritico = limitesCriticos.getOrDefault("Disco%", 95000) / 1000;
            int limiteDiscoAtencao = limitesAtencao.getOrDefault("Disco%", 85000) / 1000;

            int limiteUsoRedeCritico = limitesCriticos.getOrDefault("Rede%", 90000) / 1000;
            int limiteUsoRedeAtencao = limitesAtencao.getOrDefault("Rede%", 80000) / 1000;

            int xlimiteLatenciaCritico = limitesCriticos.getOrDefault("Redems", 50);
            int xlimiteLatenciaAtencao = limitesAtencao.getOrDefault("Redems", 40);
            Double limiteLatenciaCritico = parseDouble(String.valueOf(xlimiteLatenciaCritico)) / 1000;
            Double limiteLatenciaAtencao = parseDouble(String.valueOf(xlimiteLatenciaAtencao)) / 1000;


            String statusCpu = getNivelAlerta(cpu, limiteCpuAtencao, limiteCpuCritico);
            if (cpu >= limiteCpuAtencao) {
                contCPU++;
                if (cpu >= limiteCpuCritico) {
                    criticoCPU = true;
                }
            } else {
                criticoCPU = false;
                contCPU = 0;
            }

            if (contCPU == 3) {
                if (criticoCPU) {
                    criarAlertaCritico(id, "CPU", timestamp, cpu, Integer.toString(limiteCpuCritico), "%");
                } else {
                    criarAlertaAtencao(id, "CPU", timestamp, cpu, Integer.toString(limiteCpuAtencao), "%");
                }
            }

            String statusRam = getNivelAlerta(ram, limiteRamAtencao, limiteRamCritico);
            if (ram >= limiteRamAtencao) {
                contRAM++;
                if (ram >= limiteRamCritico) {
                    criticoRAM = true;
                }
            } else {
                criticoRAM = false;
                contRAM = 0;
            }
            if (contRAM == 3) {
                if (criticoRAM) {
                    criarAlertaCritico(id, "Memória RAM", timestamp, ram, Integer.toString(limiteRamCritico), "%");
                } else {
                    criarAlertaAtencao(id, "Memória RAM", timestamp, ram, Integer.toString(limiteRamAtencao), "%");
                }
            }

            String statusDisco = getNivelAlerta(disco, limiteDiscoAtencao, limiteDiscoCritico);
            if (disco >= limiteDiscoAtencao) {
                contDisco++;
                if (disco >= limiteDiscoCritico) {
                    criticoDisco = true;
                }
            } else {
                criticoDisco = false;
                contDisco = 0;
            }
            if (contDisco == 3) {
                if (criticoDisco) {
                    criarAlertaCritico(id, "Disco", timestamp, disco, Integer.toString(limiteDiscoCritico), "%");
                } else {
                    criarAlertaAtencao(id, "Disco", timestamp, disco, Integer.toString(limiteDiscoAtencao), "%");
                }
            }

            String statusUsoRede = getNivelAlerta(usoRede, limiteUsoRedeAtencao, limiteUsoRedeCritico);
            if (usoRede >= limiteUsoRedeAtencao) {
                contUsoRede++;
                if (usoRede >= limiteUsoRedeCritico) {
                    criticoUsoRede = true;
                }
            } else {
                criticoUsoRede = false;
                contUsoRede = 0;
            }
            if (contUsoRede == 3) {
                if (criticoUsoRede) {
                    criarAlertaCritico(id, "Uso de Rede", timestamp, usoRede, Integer.toString(limiteUsoRedeCritico), "MB");
                } else {
                    criarAlertaAtencao(id, "Uso de Rede", timestamp, usoRede, Integer.toString(limiteUsoRedeCritico), "MB");
                }
            }

            String statusLatencia = getNivelAlerta(latencia, limiteLatenciaAtencao, limiteLatenciaCritico);
            if (latencia >= limiteLatenciaAtencao) {
                contLatencia++;
                if (latencia >= limiteLatenciaCritico) {
                    criticoLatencia = true;
                }
            } else {
                criticoLatencia = false;
                contLatencia = 0;
            }
            if (contLatencia == 3) {
                if (criticoLatencia) {
                    criarAlertaCritico(id, "Latência", timestamp, latencia, Double.toString(xlimiteLatenciaCritico), "ms");
                } else {
                    criarAlertaAtencao(id, "Latência", timestamp, latencia, Double.toString(xlimiteLatenciaCritico), "ms");
                }
            }


            dadosTratados.add(new String[] {
                    String.valueOf(id), timestamp,
                    String.valueOf(cpu),
                    String.valueOf(ram),
                    String.valueOf(disco),
                    String.valueOf(usoRede),
                    String.valueOf(latencia),
                    String.valueOf(qtdProcessos),
                    statusCpu, statusRam, statusDisco, statusUsoRede, statusLatencia
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
