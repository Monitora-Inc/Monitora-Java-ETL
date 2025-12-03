package com.monitora.dc;
import com.monitora.dc.model.MetricaBruta;
import com.monitora.dc.model.MetricaTratada;

public class Transformar {

    public MetricaTratada transformar(MetricaBruta mb, Double limiteMb) {
        MetricaTratada mt = new MetricaTratada();
        mt.idServidor = mb.idServidor;
        mt.timestamp = mb.timestamp;
        mt.cpuPercent = parseDouble(mb.cpu);
        mt.ramPercent = parseDouble(mb.ramPercent);
        mt.discoPercent = parseDouble(mb.discoPercent);
        mt.redeMb = parseDouble(mb.usoRedeMb);
        mt.redeMbS = ((mt.redeMb / 3) / limiteMb) * 100;
        mt.latenciaMs = parseDouble(mb.latencia) * 1000.0;
        mt.processos = parseInt(mb.qtdProcessos);
        mt.saude = calcularSaude(mt);
        definirEstadoVisual(mt);
        return mt;
    }

    private double parseDouble(String v) {
        try {
            return Double.parseDouble(v);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private int parseInt(String v) {
        try {
            return Integer.parseInt(v);
        } catch (Exception e) {
            return 0;
        }
    }

    private double calcularSaude(MetricaTratada t) {
        double saude = 100;
        saude -= t.cpuPercent * 0.15;
        saude -= t.ramPercent * 0.25;
        saude -= t.discoPercent * 0.20;
        saude -= (t.latenciaMs / 1000.0) * 0.40;
        if (saude < 0) saude = 0;
        if (saude > 100) saude = 100;
        return saude;
    }

    private void definirEstadoVisual(MetricaTratada t) {
        if (t.saude >= 90) {
            t.up = true;
            t.cor = "#00D4C4";
        }
        else if (t.saude >= 75) {
            t.up = true;
            t.cor = "#FFB94A";
        }
        else {
            t.up = false;
            t.cor = "#FF3F72";
        }
    }
}