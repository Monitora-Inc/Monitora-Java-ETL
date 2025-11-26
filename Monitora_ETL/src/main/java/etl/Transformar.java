package etl;

import java.util.*;

import static etl.JiraIssueCreator.criarAlertaCritico;
import static etl.JiraIssueCreator.criarAlertaAtencao;

public class Transformar {
    private Map<String, Double> limitesCriticos;
    private Map<String, Double> limitesAtencao;

    public Transformar(Map<String, Double> limitesCriticos, Map<String, Double> limitesAtencao) {
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
            if (col == null || col.length < 17)
                continue;

            // ignora linha de cabeçalho
            if (col[0] == null || "id".equalsIgnoreCase(col[0]))
                continue;

            boolean temNulo = false;
            for (int i = 0; i < 17; i++) {
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
            double total_ram = parseDouble(col[3]);
            double ram_usada = parseDouble(col[4]);
            double ram_percent = parseDouble(col[5]);
            double ram_quente = parseDouble(col[6]);
            double ram_fria = parseDouble(col[7]);
            double disco = parseDouble(col[8]);
            int bytesEnv= (int) parseDouble(col[9]);
            int bytesRecb= (int) parseDouble(col[10]);
            double usoRede = (bytesEnv + bytesRecb)/(1048576);
            double latencia = parseDouble(col[11]);
            int pacotes_enviados =(int) parseDouble(col[12]);
            int pacotes_recebidos =(int) parseDouble(col[13]);
            int pacotes_perdidos =(int) parseDouble(col[14]);
            int qtdProcessos = (int) parseDouble(col[15]);
            int uptime_segundos = (int) parseDouble(col[16]);



            // busca limites do banco mas se não houver define valores padrao


            Double limiteCpuCritico = limitesCriticos.get("CPU%");
            Double limiteCpuAtencao = limitesAtencao.get("CPU%");
            if (limiteCpuAtencao == null){
                limiteCpuAtencao = 80.0;
            }
            if (limiteCpuCritico == null){
                limiteCpuCritico = 90.0;
            }

            Double limiteRamCritico = limitesCriticos.get("RAM%");
            Double limiteRamAtencao = limitesAtencao.get("RAM%");
            if (limiteRamCritico == null){
                limiteRamCritico = 90.0;
            }
            if (limiteRamAtencao == null){
                limiteRamAtencao = 80.0;
            }

            Double limiteDiscoCritico = limitesCriticos.get("Disco%");
            Double limiteDiscoAtencao = limitesAtencao.get("Disco%");
            if (limiteDiscoAtencao == null){
                limiteDiscoAtencao = 80.0;
            }
            if (limiteDiscoCritico == null){
                limiteDiscoCritico = 90.0;
            }
            Double limiteUsoRedeCritico = limitesCriticos.get("Redemb");
            Double limiteUsoRedeAtencao = limitesAtencao.get("Redemb");
            if (limiteUsoRedeAtencao == null){
                limiteUsoRedeAtencao = 80.0;
            }
            if (limiteUsoRedeCritico == null){
                limiteUsoRedeCritico = 90.0;
            }
            Double limiteLatenciaCritico = limitesCriticos.get("Redems");
            Double limiteLatenciaAtencao = limitesAtencao.get("Redems");
            if (limiteLatenciaAtencao == null){
                limiteLatenciaAtencao = 0.05;
            }
            if (limiteLatenciaCritico == null){
                limiteLatenciaCritico = 0.07;
            }
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

            if (contCPU == 5) {
                if (criticoCPU) {
                    criarAlertaCritico(id, "CPU", timestamp);
                } else {
                    criarAlertaAtencao(id, "CPU", timestamp);
                }
            }

            String statusRam = getNivelAlerta(ram_percent, limiteRamAtencao, limiteRamCritico);
            if (ram_percent >= limiteRamAtencao) {
                contRAM++;
                if (ram_percent >= limiteRamCritico) {
                    criticoRAM = true;
                }
            } else {
                criticoRAM = false;
                contRAM = 0;
            }
            if (contRAM == 5) {
                if (criticoRAM) {
                    criarAlertaCritico(id, "Memória RAM", timestamp);
                } else {
                    criarAlertaAtencao(id, "Memória RAM", timestamp);
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
            if (contDisco == 5) {
                if (criticoDisco) {
                    criarAlertaCritico(id, "Disco", timestamp);
                } else {
                    criarAlertaAtencao(id, "Disco", timestamp);
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
            if (contUsoRede == 5) {
                if (criticoUsoRede) {
                    criarAlertaCritico(id, "Uso de Rede", timestamp);
                } else {
                    criarAlertaAtencao(id, "Uso de Rede", timestamp);
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
            if (contLatencia == 5) {
                if (criticoLatencia) {
                    criarAlertaCritico(id, "Latência", timestamp);
                } else {
                    criarAlertaAtencao(id, "Latência", timestamp);
                }
            }

            dadosTratados.add(new String[] {
                    id,
                    timestamp,
                    String.valueOf(cpu),
                    String.valueOf(total_ram),
                    String.valueOf(ram_usada),
                    String.valueOf(ram_percent),
                    String.valueOf(ram_quente),
                    String.valueOf(ram_fria),
                    String.valueOf(disco),
                    String.valueOf(bytesEnv),
                    String.valueOf(bytesRecb),
                    String.valueOf(usoRede),
                    String.valueOf(latencia),
                    String.valueOf(pacotes_enviados),
                    String.valueOf(pacotes_recebidos),
                    String.valueOf(pacotes_perdidos),
                    String.valueOf(qtdProcessos),
                    String.valueOf(uptime_segundos),
                    statusCpu,
                    statusRam,
                    statusDisco,
                    statusUsoRede,
                    statusLatencia
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
